# ----------------------------------------------------------------------------------------------------------------------
# init_data.py builds and stores pandas dataframes for all shots and shot assists in each season of football contained
# in the Understat database (100+ megabytes).
# cd desktop/fpl_ml
# source venv/bin/activate
# python init_data.py
# ----------------------------------------------------------------------------------------------------------------------

import os
import numpy as np
import itertools as it
import time
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
import asyncio
import aiohttp
from understat import Understat

# Initialise names of leagues and seasons to be gathered
leagues, seasons = ['epl', 'la_liga', 'bundesliga', 'serie_A', 'ligue_1', 'rfpl'], np.arange(2014, 2021)
root_dir = os.path.dirname(os.path.abspath(__file__))





# build_understat_df initialises pandas dataframe compiling all shot data from previous seasons
def build_shot_df():
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            team_dict = {league: {season: set() for season in seasons} for league in leagues}
            existing_files = 0
            shutil.rmtree(os.path.join(root_dir, 'raw_data'), ignore_errors=True)
            for league in leagues:
                shot_dict, player_ids = {}, set()
                for season in seasons:
                    # Creates path to store pickle and csv files for each league/season
                    path = '{}/raw_data/{}/{}'.format(root_dir, league, str(season))
                    if not os.path.isfile('{}/{}_{}_df.pickle'.format(path, league, season)):
                        Path(path).mkdir(parents=True, exist_ok=True)
                        # Adds players from each team in given season to player_ids
                        for team in await understat.get_teams(league, season):
                            team_dict[league][season].add(team['title'])
                            print('{} squad ({})'.format(team['title'], season))
                            print('\n')
                            for player in await understat.get_team_players(team['title'], season):
                                print(player['player_name'])
                                player_ids.add(int(player['id']))
                            print('\n' * 2)
                    else:
                        existing_files += 1
                        if existing_files == len(leagues) * len(seasons):  #  If all relevant .pickle files exist
                            print('Initial data requirements met')

                count, last_percent = 0, -1
                # Adds shot data for all players in player_ids across all leagues/seasons to shot_dict
                for player_id in player_ids:
                    percent = int(count / len(player_ids) * 100)
                    if percent != last_percent:
                        print('{}% of {} data gathered'.format(int(count / len(player_ids) * 100),
                                                               league.replace('_', ' ').title()))
                    count += 1
                    last_percent = percent
                    if player_id != 7798:  # Unresolved aiohttp bug occurs when attempting to access 7798's data
                        # (possible IDE-related)
                        # Iterates through each shot and gathers relevant data points
                        for shot_info in await understat.get_player_shots(player_id):
                            if datetime.strptime(shot_info['date'], '%Y-%m-%d %H:%M:%S'):
                                shot_dict[int(shot_info['id'])] = {
                                    'shot_type': shot_info['shotType'],
                                    'situation': shot_info['situation'],
                                    'result': shot_info['result'],
                                    'xG': float(shot_info['xG']),
                                    'minute': int(shot_info['minute']),
                                    'team_score': shot_info['{}_goals'.format(shot_info['h_a'])],
                                    'opponent_score': shot_info['{}_goals'.format(['h', 'a'][shot_info['h_a'] == 'h'])],
                                    'name': shot_info['player'],
                                    'understat_id': int(shot_info['player_id']),
                                    'location': shot_info['h_a'],
                                    'team': shot_info['{}_team'.format(shot_info['h_a'])],
                                    'opponent': shot_info['{}_team'.format(['h', 'a'][shot_info['h_a'] == 'h'])],
                                    'assist': shot_info['player_assisted'],
                                    'date': datetime.strptime(shot_info['date'], '%Y-%m-%d %H:%M:%S'),
                                    'match_id': int(shot_info['match_id']),
                                    'season': int(shot_info['season']),
                                    'position': (float(shot_info['X']), float(shot_info['Y']))
                                }
                df = pd.DataFrame(shot_dict).T  # Converts shot_dict to transposed pandas dataframe
                # Creates files for each season of league if .pickle file does not yet exist
                for season in seasons:
                    for team in team_dict[league][season]:
                        path = '{}/raw_data/{}/{}/{}/shots'.format(root_dir, league, season, team)
                        if not os.path.isfile('{}/{}_{}_shot_df.pickle'.format(path, team, season)):
                            Path(path).mkdir(parents=True, exist_ok=True)
                            sub_df = df[(df['season']==season) & (df['team']==team)].sort_values(by='date')
                            sub_df.to_csv('{}/{}_{}_shot_df.csv'.format(path, team, season))
                            sub_df.to_pickle('{}/{}_{}_shot_df.pickle'.format(path, team, season))


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


# build_players_df builds a referential database of players' full names and their understat ID
def build_players_df():
    players = []
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            # Imports players' name and understat ID by season by league
            for league, season in it.product(leagues, seasons):
                print('Importing {} {} players'.format(league.replace('_', ' ').title(), season))
                for player in await understat.get_league_players(league, season):
                    players.append({'name': player['player_name'], 'understat_id': int(player['id'])})
            df = pd.DataFrame(players).drop_duplicates() # Delete duplicate entries
            # Creates files for each season of league if .pickle file does not yet exist
            if not os.path.isfile('{}/raw_data/player_df.pickle'.format(root_dir)):
                df.to_csv('{}/raw_data/player_df.csv'.format(root_dir))
                df.to_pickle('{}/raw_data/player_df.pickle'.format(root_dir))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


# build_result_df builds a nested dict containing pd.DataFrame of results by team by season by league
def build_results_df():
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            result_dict = {league: {season: {} for season in seasons} for league in leagues} # Initialise dict for df
            # Imports data for each result by season by league and gathers relevant data
            for league, season in it.product(leagues, seasons):
                print('Importing {} {} results'.format(league.replace('_', ' ').title(), season))
                teams = set()
                # Creates dict containing teams by season by league
                for team_info in await understat.get_teams(league, season):
                    teams.add(team_info['title'])
                    result_dict[league][season][team_info['title']] = {}
                # Iterates through season results to insert relevant data about the result into dict
                for location, result_info in it.product(['h', 'a'], await understat.get_league_results(league, season)):
                    if datetime.strptime(result_info['datetime'], '%Y-%m-%d %H:%M:%S'):
                        result_dict[league][season][result_info[location]['title']][int(result_info['id'])] = {
                            'team': result_info[location]['title'],
                            'opponent': result_info[['h', 'a'][location=='h']]['title'],
                            'xG_scored': float(result_info['xG'][location]),
                            'xG_conceded': float(result_info['xG'][['h', 'a'][location=='h']]),
                            'goals_scored': int(result_info['goals'][location]),
                            'goals_conceded': int(result_info['goals'][['h', 'a'][location=='h']]),
                            'location': location,
                            'date': datetime.strptime(result_info['datetime'], '%Y-%m-%d %H:%M:%S')
                    }
                # For each team in given league season, creates results df if they do not yet exist
                for team in teams:
                    path = '{}/raw_data/{}/{}/{}/results'.format(root_dir, league, season, team)
                    if not os.path.isfile('{}/{}_{}_results_df.pickle'.format(path, season, team)):
                        Path(path).mkdir(parents=True, exist_ok=True)
                        if season == 2020:
                            print(pd.DataFrame(result_dict[league][season][team]).T)
                        df = pd.DataFrame(result_dict[league][season][team]).T.sort_values(by='date')
                        df.to_pickle('{}/{}_{}_result_df.pickle'.format(path, team, season))
                        df.to_csv('{}/{}_{}_result_df.csv'.format(path, team, season))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


# build_data condenses all initial data-gathering processes into one function
def build_data():
    build_shot_df()
    build_results_df()
    build_players_df()


# update_data provides a more efficient way of adding new data to data set than rebuilding data set
def update_data():
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            # Iterates through current season of each league to add any matches not already contained in raw_data
            for league in leagues:
                existing_ids = set()
                path = '{}/raw_data/{}/{}'.format(root_dir, league, seasons[-1])
                # Gathers set existing_ids containing IDs of matches already in raw_data
                for sub_dir, dirs, files in os.walk(path):
                    for file in files:
                        if file.endswith('result_df.pickle'):
                            df = pd.read_pickle(os.path.join(sub_dir, file))
                            existing_ids.update(df.index.to_list())
                # Appends shot and result data for matches not in existing_ids
                for result_info in await understat.get_league_results(league, seasons[-1]):
                    if int(result_info['id']) not in existing_ids:
                        print('{} vs. {} appending...'.format(result_info['h']['title'], result_info['a']['title']))
                        for location in ('h', 'a'):
                            team = result_info[location]['title']
                            shot_df = pd.read_pickle(
                                '{0}/{1}/shots/{1}_{2}_shot_df.pickle'.format(path, team, seasons[-1]))
                            result_df = pd.read_pickle(
                                '{0}/{1}/results/{1}_{2}_result_df.pickle'.format(path, team, seasons[-1]))
                            try:
                                shots = await understat.get_match_shots(result_info['id'])
                            except :
                                print('{} vs. {} failed to append'.format(result_info['h']['title'], result_info['a']['title']))
                                break
                            new_shot_dict = {}
                            for shot_info in shots[location]:
                                new_shot_dict[shot_info['id']] = {
                                    'shot_type': shot_info['shotType'],
                                    'situation': shot_info['situation'],
                                    'result': shot_info['result'],
                                    'xG': float(shot_info['xG']),
                                    'minute': int(shot_info['minute']),
                                    'team_score': shot_info['{}_goals'.format(shot_info['h_a'])],
                                    'opponent_score': shot_info['{}_goals'.format(['h', 'a'][shot_info['h_a'] == 'h'])],
                                    'name': shot_info['player'],
                                    'understat_id': int(shot_info['player_id']),
                                    'location': shot_info['h_a'],
                                    'team': shot_info['{}_team'.format(shot_info['h_a'])],
                                    'opponent': shot_info['{}_team'.format(['h', 'a'][shot_info['h_a'] == 'h'])],
                                    'assist': shot_info['player_assisted'],
                                    'date': pd.Timestamp(shot_info['date']),
                                    'match_id': int(shot_info['match_id']),
                                    'season': int(shot_info['season']),
                                    'position': (float(shot_info['X']), float(shot_info['Y']))
                                }

                            new_shot_df = pd.DataFrame(new_shot_dict).T
                            shot_df = pd.concat([shot_df, new_shot_df])

                            shot_df.sort_values(by='date').to_pickle(
                                '{0}/{1}/shots/{1}_{2}_shot_df.pickle'.format(path, team, seasons[-1]))
                            shot_df.sort_values(by='date').to_csv(
                                '{0}/{1}/shots/{1}_{2}_shot_df.csv'.format(path, team, seasons[-1]))

                            result_df.loc[int(result_info['id'])] = {
                                'team': team,
                                'opponent': result_info[['h', 'a'][location == 'h']]['title'],
                                'xG_scored': float(result_info['xG'][location]),
                                'xG_conceded': float(result_info['xG'][['h', 'a'][location == 'h']]),
                                'goals_scored': int(result_info['goals'][location]),
                                'goals_conceded': int(result_info['goals'][['h', 'a'][location == 'h']]),
                                'location': location,
                                'date': datetime.strptime(result_info['datetime'], '%Y-%m-%d %H:%M:%S')
                            }
                            result_df.sort_values(by='date').to_pickle(
                                '{0}/{1}/results/{1}_{2}_result_df.pickle'.format(path, team, seasons[-1]))
                            result_df.sort_values(by='date').to_csv(
                                '{0}/{1}/results/{1}_{2}_result_df.csv'.format(path, team, seasons[-1]))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())




while True:
    try:
        if not os.path.isdir('raw_data'):
            build_data()
        else:
            update_data()
        print('Data collection complete on {}'.format(datetime.now()))
        time.sleep(30)
    except KeyboardInterrupt:
        break
