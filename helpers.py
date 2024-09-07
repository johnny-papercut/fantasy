import datetime
import json
import math
import threading
import time

import pytz
import requests
from bs4 import BeautifulSoup
from espn_api.football import League
from espn_api.requests.espn_requests import ESPNAccessDenied
from google.cloud import bigquery


NO_GAMETIME = datetime.datetime(2000, 1, 1, tzinfo=pytz.timezone("America/Chicago"))
TABLES = {
    'leagues': 'commander.leagues',
    'teams': 'commander.teams',
    'projections': 'commander.projections',
    'scores': 'commander.scores',
    'matchups': 'commander.matchups',
    'game_progress': 'commander.game_progress',
    'changes': 'commander.changes',
}


def initialize_bigquery_client():
    return bigquery.Client()


def run_query(query: str, as_list: bool = False):
    if not as_list:
        return bigquery.Client().query(query).result()
    else:
        return [row for row in bigquery.Client().query(query).result()]


def write_to_bigquery(table: str, schema: list, rows: list):

    bq = bigquery.Client()

    job_config = bigquery.LoadJobConfig(schema=schema, source_format='NEWLINE_DELIMITED_JSON')
    bq.load_table_from_json(rows, table, job_config=job_config).result()


def load_profiles() -> dict:

    profiles = {}
    bq = bigquery.Client()

    for league in [league for league in bq.query(f"SELECT * FROM `{TABLES.get('leagues')}` ORDER BY platform, league_id").result()]:

        if league.profile not in profiles.keys():
            profiles[league.profile] = []

        profiles[league.profile].append({
            'name': league.name,
            'platform': league.platform,
            'scoring': league.scoring,
            'league_id': league.league_id,
            'team_id': league.team_id,
            'start_year': league.start_year,
            'swid': league.swid,
            's2': league.s2,
        })
    
    return profiles


def initialize_espn_league(league_id: int, year: int) -> League:

    s2 = swid = None
    profiles = load_profiles()

    for profile, leagues in profiles.items():
        for league in leagues:
            if league.get('league_id') == league_id:
                s2, swid = league.get('s2'), league.get('swid')

    return League(league_id=league_id, year=year, espn_s2=s2, swid=swid)


def get_current_week() -> int:
    season_start = datetime.datetime(2024, 9, 5, tzinfo=pytz.timezone("America/Chicago"))
    delta = get_current_central_datetime() - season_start
    return int(delta.days / 7) + 1


def get_current_year() -> int:
    return datetime.datetime.utcnow().year


def get_current_central_datetime() -> datetime.datetime:
    return datetime.datetime.now(pytz.timezone('America/Chicago'))


def player_sort(item: dict) -> tuple:
    sorting_order = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 4, 'DST': 5, 'K': 6, 'BE': 10, 'IR': 11}
    try:
        return sorting_order.get(item.get('position'), 2)
    except TypeError as e:
        return 0


def translate_team(input: str, output: str, team_name: str) -> str:

    teams = [
        {'espn': 'WSH', 'sleeper': 'WAS', 'fp': 'WAS', 'nfl': 'WSH'},
        {'espn': 'JAX', 'sleeper': 'JAX', 'fp': 'JAC', 'nfl': 'JAX'},
        {'espn': 'OAK', 'sleeper': 'LV', 'fp': 'LV', 'nfl': 'LV'},
    ]

    if not team_name:
        return ''

    for team in teams:
        if team.get(input) == team_name:
            return team.get(output)
    
    return team_name


def get_all_projections(week: int = get_current_week()) -> dict:

    runtime = datetime.datetime.utcnow()

    projections = {}

    for position_name in ['qb', 'rb', 'wr', 'te', 'k', 'dst']:
        for scoring in ['half-point-ppr', 'ppr']:

            if position_name in ['qb', 'k', 'dst']:
                url = f"https://www.fantasypros.com/nfl/rankings/{position_name}.php?week={week}"
            else:
                url = f"https://www.fantasypros.com/nfl/rankings/{scoring}-{position_name}.php?week={week}"

            for line in BeautifulSoup(requests.get(url).text, 'html.parser').find_all('script'):
                if 'ecrData' in line.text:

                    data = json.loads(line.text.split('\n')[5].split('var ecrData = ')[1].replace(';', ''))

                    for player in data.get('players'):

                        if position_name != 'dst':
                            name = ' '.join(player.get('player_name').split(' ')[0:2])
                        else:
                            name = f"{player.get('player_name').split(' ')[-1]} D/ST"
                        team = player.get('player_team_id')
                        position = player.get('player_position_id')
                        projected = player.get('r2p_pts')

                        if not projected:
                            continue

                        if team not in projections.keys():
                            projections[team] = {}
                        
                        if position not in projections.get(team).keys():
                            projections[team][position] = {}

                        if name not in projections.get(team).get(position).keys():
                            projections[team][position][name] = {}

                        projections[team][position][name][scoring] = float(projected)

    return projections


def update_all_scores(week: int = get_current_week()) -> dict:

    runtime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    leagues = []
    matchups = []
    gametimes = {}
    responses = []

    schemas = {
        'scores': [
            {"name": "league_id",   "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "team_id",     "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "week",        "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "name",        "type": "STRING",   "mode": "REQUIRED"},
            {"name": "team",        "type": "STRING",   "mode": "REQUIRED"},
            {"name": "status",      "type": "STRING",   "mode": "REQUIRED"},
            {"name": "position",    "type": "STRING",   "mode": "REQUIRED"},
            {"name": "slot",        "type": "STRING",   "mode": "REQUIRED"},
            {"name": "points",      "type": "FLOAT",    "mode": "REQUIRED"},
            {"name": "play_status", "type": "STRING",   "mode": "REQUIRED"},
            {"name": "gametime",    "type": "DATETIME", "mode": "REQUIRED"},
            {"name": "updated",     "type": "DATETIME", "mode": "REQUIRED"},
        ],
        'matchups': [
            {"name": "league_id",   "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "week",        "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "home",        "type": "INTEGER",  "mode": "REQUIRED"},
            {"name": "away",        "type": "INTEGER",  "mode": "REQUIRED"},
        ],
    }

    for profile in load_profiles().values():
        for league in profile:
            if (league.get('platform'), league.get('league_id')) not in leagues:
                leagues.append((league.get('platform'), league.get('league_id')))

    for league in leagues:

        platform = league[0]
        league_id = league[1]

        players = []

        if platform == 'espn':

            league = initialize_espn_league(league_id, 2024)

            for game in league.box_scores(week):

                matchups.append({'league_id': league_id, 'week': week, 'home': game.home_team.team_id, 'away': game.away_team.team_id})
                matchups.append({'league_id': league_id, 'week': week, 'home': game.away_team.team_id, 'away': game.home_team.team_id})
                
                for team_data, team_roster in ((game.home_team, game.home_lineup), (game.away_team, game.away_lineup)):
                    for player_data in team_roster:

                        player = {
                            'league_id': league_id,
                            'week': week,
                            'team_id': team_data.team_id,
                            'name': player_data.name,
                            'team': player_data.proTeam,
                            'status': player_data.injuryStatus,
                            'position': player_data.position.replace('/', ''),
                            'slot': player_data.slot_position.replace('/', '').replace('RBWRTE', 'FLEX'),
                            'points': player_data.points,
                        }

                        if player.get('status') == 'NORMAL':
                            player['status'] = 'ACTIVE'

                        if player.get('projected') == 0 and player.get('status') == 'ACTIVE':
                            player['status'] = 'warning'

                        if not hasattr(player_data, 'game_date'):
                            player['gametime'] = NO_GAMETIME
                            player['play_status'] = 'bye'
                        
                        else:
                            player['gametime'] = player_data.game_date.astimezone(pytz.timezone('America/Chicago'))
                            now = get_current_central_datetime()
                            if now >= player.get('gametime'):
                                player['play_status'] = 'played' if player_data.game_played == 100 else 'playing'
                            elif player.get('gametime').strftime('%Y-%m-%d') == now.strftime('%Y-%m-%d'):
                                player['play_status'] = 'today'
                            else:
                                player['play_status'] = 'future'

                        if player.get('gametime') and player_data.proTeam not in gametimes.keys():
                            gametimes[player_data.proTeam] = (player.get('gametime'), player_data.game_played == 100)

                        player['gametime'] = player.get('gametime').strftime('%Y-%m-%d %H:%M:%S')
                        player['updated'] = runtime

                        players.append(player)

        if platform == 'sleeper':

            all_players = requests.get('https://api.sleeper.app/v1/players/nfl').json()

            count = 0

            matchup = []
            players = []

            for team in sorted(
                requests.get(f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}').json(),
                key=lambda x: x.get('matchup_id')):

                for i in team.get('players'):

                    player_data = all_players.get(i)

                    if not player_data:
                        continue

                    player = {
                        'league_id': league_id,
                        'week': week,
                        'team_id': team.get('roster_id'),
                        'name': player_data.get('full_name', f"{player_data.get('last_name')} D/ST"),
                        'team': translate_team('sleeper', 'espn', player_data.get('team')),
                        'status': player_data.get('injury_status'),
                        'position': player_data.get('fantasy_positions')[0].replace('DEF', 'DST'),
                        'slot': player_data.get('fantasy_positions')[0].replace('DEF', 'DST') if i in team.get('starters') else 'BE',
                        'points': team.get('players_points').get(i),
                    }

                    if player.get('status') == None:
                        player['status'] = 'ACTIVE'

                    if player.get('projected') == 0 and player.get('status') == 'ACTIVE':
                        player['status'] = 'warning'
                    
                    gametime, gamedone = gametimes.get(translate_team('sleeper', 'espn', player_data.get('team')), (None, None))

                    if not gametime or gametime == NO_GAMETIME:
                        player['gametime'] = NO_GAMETIME
                        player['play_status'] = 'bye'
                    
                    else:
                        player['gametime'] = gametime
                        now = get_current_central_datetime()
                        if now >= player.get('gametime'):
                            player['play_status'] = 'played' if gamedone else 'playing'
                        elif player.get('gametime').strftime('%Y-%m-%d') == now.strftime('%Y-%m-%d'):
                            player['play_status'] = 'today'
                        else:
                            player['play_status'] = 'future'

                    player['gametime'] = player.get('gametime').strftime('%Y-%m-%d %H:%M:%S')
                    player['updated'] = runtime

                    players.append(player)
                
                matchup.append(team.get('roster_id'))

                count += 1

                if not count % 2:
                    matchups.append({'league_id': league_id, 'week': week, 'home': matchup[0], 'away': matchup[1]})
                    matchups.append({'league_id': league_id, 'week': week, 'home': matchup[1], 'away': matchup[0]})
                    matchup = []
        
        for player in players:
            for suffix in [' Jr.', ' III']:
                if suffix in player.get('name'):
                    player['name'] = player.get('name').replace(suffix, '')

        if players:
            write_to_bigquery(TABLES.get('scores'), schemas.get('scores'), players)
            run_query(f"DELETE FROM `{TABLES.get('scores')}` WHERE league_id = {league_id} AND updated < '{runtime}'")
    
    if matchups:
        run_query(f"DELETE FROM `{TABLES.get('matchups')}` WHERE week = {week}")
        write_to_bigquery(TABLES.get('matchups'), schemas.get('matchups'), matchups)


def get_league_data(data: dict, league: dict):

    data[league.get('name')] = []
    threads = []

    if league.get('platform') == 'espn':

        for year in range(league.get('start'), datetime.datetime.utcnow().year + 1):

            thread = threading.Thread(target=get_league_year_data, args=(data, year, league))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()


def get_league_year_data(data: dict, year: int, league: dict):

    threads = []
    season = None

    while not season:
        try:
            season = initialize_espn_league(league.get('id'), year)
        except ESPNAccessDenied:
            time.sleep(0.5)


    week = 1

    for week in range(1, 15):
        
        thread = threading.Thread(target=get_league_week_data, args=(data, year, week, season, league))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def get_league_week_data(data: dict, year: int, week: int, season: League, league: dict):

    threads = []
    matchup_data = None

    while not matchup_data:
        try:
            matchup_data = season.box_scores(week)
        except ESPNAccessDenied:
            time.sleep(0.5)
    
    if not matchup_data or matchup_data[0].is_playoff:
        return
        
    if year >= datetime.datetime.utcnow().year and week >= get_current_week():
        return

    matchup_id = 0

    for matchup in matchup_data:

        matchup_id += 1

        for team in (
            (matchup.home_team, matchup.home_score, matchup.home_projected),
            (matchup.away_team, matchup.away_score, matchup.away_projected)
        ):

            if team[1] == 0:
                continue

            owner = "Redacted" if team[0].owner == "None" else team[0].owner

            data[league.get('name')].append(
                (year, week, matchup_id, owner, round(team[1], 2), round(team[2], 2), round(team[1] - team[2], 2))
            )


def cleanup(text: str) -> str:
    return ' '.join(c.capitalize() for c in text.split()).strip().replace('  ', ' ')


def organize_team(players: list, mode: str = 'default', flex_count = 1) -> dict:

    team = {'starters': [], 'bench': [], 'points': 0, 'projected': 0}

    for player in players:
        if 'D/ST' not in player.get('name'):
            player['name'] = f"{player.get('name').split()[0][0]}. {' '.join(player.get('name').split()[1:])}"
        if player.get('play_status') in ['played', 'playing']:
            player['display'] = player.get('points')
        else:
            player['display'] = '--'
        team['bench' if player.get('slot') in ['BE', 'IR'] else 'starters'].append(player)

    team['starters'] = sorted(team.get('starters'), key=player_sort)
    team['bench'] = sorted(team.get('bench'), key=player_sort)
    team['show'] = []

    if mode == 'max':
    
        ordered_players = sorted(team.get('starters') + team.get('bench'), key=lambda x: x.get('projected', 0), reverse=True)

        for position in ['QB', 'RB', 'WR', 'TE', 'FLEX', 'DST', 'K']:
            if position != 'FLEX':
                team['show'].append((position, [p for p in ordered_players if p.get('position') == position][0]))

            else:
                for p in ordered_players:
                    if p.get('position') in ['RB', 'WR', 'TE'] and p not in [op[1] for op in team.get('show')]:
                        team['show'].append((position, p))
                        break

            if position in ['RB', 'WR']:
                team['show'].append((position, [p for p in ordered_players if p.get('position') == position][1]))

            if flex_count == 2 and position == 'FLEX':
                for p in ordered_players:
                    if p.get('position') in ['RB', 'WR', 'TE'] and p not in [op[1] for op in team.get('show')]:
                        team['show'].append((position, p))
                        break
        
        team['show'] = [p[1] for p in team.get('show')]
    
    elif mode == 'default':

        team['show'] = team.get('starters')
    
    elif mode == 'all':
    
        team['show'] = team.get('starters')
        team['show'].extend(team.get('bench'))

    for player in team.get('show'):
        team['points'] += player.get('points')
        team['projected'] += player.get('projected')

    team['projected'] = round(team.get('projected'), 2)

    return team


def get_all_matchups(profile_name: str, week: int, mode: str = 'default') -> list:

    runtime = datetime.datetime.utcnow()

    leagues = load_profiles().get(profile_name)
    matchups = []

    if not leagues:
        return []

    score_query = f"SELECT * EXCEPT (_rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY league_id, team_id, week, name ORDER BY updated DESC) AS _rn " \
                  f"FROM `{TABLES.get('scores')}`) WHERE week = {week}"

    dbs = {
        'matchups': run_query(f"SELECT * FROM `{TABLES.get('matchups')}` WHERE week = {week}", as_list=True),
        'teams': run_query(f"SELECT * FROM `{TABLES.get('teams')}`", as_list=True),
        'projections': run_query(f"SELECT * FROM `{TABLES.get('projections')}` WHERE week = {week}", as_list=True),
        'scores': run_query(score_query, as_list=True),
        'game_progress': run_query(f"SELECT * FROM `{TABLES.get('game_progress')}` WHERE week = {week}", as_list=True),
    }

    projections = {}

    for projection in dbs.get('projections'):

        projection = dict(projection)

        projection['team'] = translate_team('fp', 'espn', projection.get('team'))

        if projection.get('team') not in projections.keys():
            projections[projection.get('team')] = {}

        projections[projection.get('team')][projection.get('player')] = {
            'standard': projection.get('standard'),
            'half-point-ppr': projection.get('half-point-ppr'),
            'ppr': projection.get('ppr')
        }
    
    dbs['projections'] = projections

    progress = {}

    for game in dbs.get('game_progress'):

        game = dict(game)

        game['team'] = translate_team('nfl', 'espn', game.get('team'))

        if progress.get('team') not in progress.keys():
            progress[game.get('team')] = {}
        
        progress[game.get('team')] = game.get('progress')
    
    dbs['progress'] = progress

    print(f"db load: {(datetime.datetime.utcnow() - runtime).seconds}s")

    for league in leagues:

        league_id = league.get('league_id')

        home = {'id': league.get('team_id'), 'players': []}
        away = {'id': 0, 'players': []}

        for matchup in dbs.get('matchups'):
            if matchup.league_id == league_id and matchup.home == home.get('id'):
                away['id'] = matchup.away
                break

        for team in dbs.get('teams'):
            if team.league_id == league_id and team.team_id == home.get('id'):
                home['team'] = team.team
                home['owner'] = team.owner
            elif team.league_id == league_id and team.team_id == away.get('id'):
                away['team'] = team.team
                away['owner'] = team.owner

        for score in dbs.get('scores'):

            score = dict(score)

            if score.get('league_id') == league_id and score.get('team_id') == home.get('id'):
                projected = dbs.get('projections').get(score.get('team'), {}).get(score.get('name'), {}).get(league.get('scoring'), 0)
                score['projected'] = calculate_projected(score, projected, progress.get(score.get('team')))
                home['players'].append(score)
            elif score.get('league_id') == league_id and score.get('team_id') == away.get('id'):
                projected = dbs.get('projections').get(score.get('team'), {}).get(score.get('name'), {}).get(league.get('scoring'), 0)
                score['projected'] = calculate_projected(score, projected, progress.get(score.get('team')))
                away['players'].append(score)

        flex_count = 2 if league.get('platform') == 'sleeper' else 1

        home['players'] = organize_team(home.get('players'), mode, flex_count)
        away['players'] = organize_team(away.get('players'), mode, flex_count)

        home['players']['winning_points'] = 'winning' if home.get('players').get('points') > away.get('players').get('points') else 'losing'
        away['players']['winning_points'] = 'winning' if away.get('players').get('points') > home.get('players').get('points') else 'losing'

        home['players']['winning_projected'] = 'winning' if home.get('players').get('projected') > away.get('players').get('projected') else 'losing'
        away['players']['winning_projected'] = 'winning' if away.get('players').get('projected') > home.get('players').get('projected') else 'losing'

        home_chance = 1 / (1 + math.exp((away.get('players').get('points') - home.get('players').get('points')) / 400))
        away_chance = 1 / (1 + math.exp((home.get('players').get('points') - away.get('players').get('points')) / 400))

        home['players']['win_chance'] = f"{round(100 * home_chance)}%"
        away['players']['win_chance'] = f"{round(100 * away_chance)}%"

        home['players']['win_chance'] = ""
        away['players']['win_chance'] = ""

        matchups.append({'home': home, 'away': away})

    return matchups


def update_projections(week: int = get_current_week()):

    runtime = get_current_central_datetime().strftime('%Y-%m-%d %H:%M:%S')
    rows = []
    changes = []
    responses = []

    old_projections = run_query(f"SELECT * FROM `{TABLES.get('projections')}` WHERE week = {week}")
    projections = get_all_projections(week)

    projections_np = {}

    remove_positions = []

    for team, team_data in projections.items():
        projections_np[team] = {}
        for position, position_data in team_data.items():
            if position not in remove_positions:
                remove_positions.append(position)
            for player_name, player_data in position_data.items():
                projections_np[team][player_name] = player_data

    for player in old_projections:
        player = dict(player)
        old = {'half-point-ppr': player.get('half-point-ppr'), 'ppr': player.get('ppr')}
        new = projections_np.get(player.get('team'), {}).get(player.get('player'), {})
        if old.get('ppr') != new.get('ppr'):
            if abs(old.get('ppr', 0) - new.get('ppr', 0)) > 3:
                changes.append({
                    'player': player.get('player'),
                    'team': player.get('team'),
                    'scoring': 'ppr',
                    'old': old.get('ppr', 0),
                    'new': new.get('ppr', 0),
                    'updated': runtime,
                })

    for team, team_data in projections.items():
        for position in team_data.values():
            for player, scoring in position.items():
                row = {
                    'player': player,
                    'team': team,
                    'week': week,
                    'standard': 0,
                    'half-point-ppr': scoring.get('half-point-ppr', 0),
                    'ppr': scoring.get('ppr', 0),
                    'updated': runtime,
                }
                rows.append(row)

    schema = [
        {"name": "player",          "type": "STRING",   "mode": "REQUIRED"},
        {"name": "team",            "type": "STRING",   "mode": "REQUIRED"},
        {"name": "week",            "type": "INTEGER",  "mode": "REQUIRED"},
        {"name": "standard",        "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "half-point-ppr",  "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "ppr",             "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "updated",         "type": "DATETIME", "mode": "REQUIRED"},
    ]

    write_to_bigquery(TABLES.get('projections'), schema, rows)
    run_query(f"DELETE FROM `{TABLES.get('projections')}` WHERE week = {week} AND updated < '{runtime}'")

    schema = [
        {"name": "player",          "type": "STRING",   "mode": "REQUIRED"},
        {"name": "team",            "type": "STRING",   "mode": "REQUIRED"},
        {"name": "scoring",         "type": "STRING",   "mode": "REQUIRED"},
        {"name": "old",             "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "new",             "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "updated",         "type": "DATETIME", "mode": "REQUIRED"},
    ]

    write_to_bigquery(TABLES.get('changes'), schema, changes)

    return True


def update_teams():

    leagues = []

    for profile in load_profiles().values():
        for league in profile:
            if league.get('league_id') not in [l.get('league_id') for l in leagues]:
                leagues.append(league)

    for league in leagues:
  
        rows = []

        if league.get('platform') == 'espn':

            url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2024/segments/0/leagues/{league.get('league_id')}?view=mTeam"

            data = requests.get(url, cookies={'espn_s2': league.get('s2'), 'swid': league.get('swid')}).json()

            owner_map = {}

            for member in data.get('members'):
                owner_map[member.get('id')] = f"{member.get('firstName')} {member.get('lastName')}"

            for team in data.get('teams'):
                rows.append({
                    'league_id': league.get('league_id'),
                    'team_id': team.get('id'),
                    'team': cleanup(team.get('name', 'None')),
                    'owner': cleanup(owner_map.get(team.get('owners', ['None'])[0], 'None')),
                })

        if league.get('platform') == 'sleeper':

            rosters = {}

            for roster in requests.get(f"https://api.sleeper.app/v1/league/{league.get('league_id')}/rosters").json():
                rosters[roster.get('owner_id')] = roster.get('roster_id')
            
            for user in requests.get(f"https://api.sleeper.app/v1/league/{league.get('league_id')}/users").json():
                if not rosters.get(user.get('user_id')):
                    continue
                rows.append({
                    'league_id': league.get('league_id'),
                    'team_id': rosters.get(user.get('user_id')),
                    'team': user.get('metadata').get('team_name') if user.get('metadata').get('team_name') else user.get('display_name'),
                    'owner': user.get('display_name'),
                })

        schema = [
            {"name": "league_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "team_id",   "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "team",      "type": "STRING",  "mode": "REQUIRED"},
            {"name": "owner",     "type": "STRING",  "mode": "REQUIRED"},
        ]

        if rows:
            run_query(f"DELETE FROM `{TABLES.get('teams')}` WHERE league_id = {league.get('league_id')}")
            write_to_bigquery(TABLES.get('teams'), schema, rows)

    return True


def update_progress():

    rows = []

    week = get_current_week()
    year = get_current_year()

    games = f"https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}&week={week}"

    for day in requests.get(games).json().get('content').get('schedule').values():
        for game in day.get('games'):
            teams = [i.get('team').get('abbreviation') for i in game.get('competitions')[0].get('competitors')]
            for team in teams:
                period = game.get('competitions')[0].get('status').get('period')
                clock = game.get('competitions')[0].get('status').get('clock')
                progress = (((period - 1) * 900) + (900 - clock)) / 3600
                display = game.get('competitions')[0].get('status').get('displayClock')
                display = f"Q{period} {'0' if len(display) < 5 else ''}{display}"
                rows.append({'year': year, 'week': week, 'team': team, 'progress': progress, 'display': display})

    schema = [
        {"name": "year",        "type": "INTEGER",  "mode": "REQUIRED"},
        {"name": "week",        "type": "INTEGER",  "mode": "REQUIRED"},
        {"name": "team",        "type": "STRING",   "mode": "REQUIRED"},
        {"name": "progress",    "type": "FLOAT",    "mode": "REQUIRED"},
        {"name": "display",     "type": "STRING",   "mode": "REQUIRED"}
    ]
    
    if rows:
        run_query(f"DELETE FROM `{TABLES.get('game_progress')}` WHERE year = {year} AND week = {week}")
        write_to_bigquery(TABLES.get('game_progress'), schema, rows)


def calculate_projected(player: dict, projection: float, progress: float) -> float:

    if progress == None or player.get('play_status') == 'bye':
        return 0

    if player.get('play_status') == 'played' or player.get('status') == 'OUT':
        return player.get('points', 0)
    
    return projection if progress < 0.25 else (player.get('points', 0) / progress)
