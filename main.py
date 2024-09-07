import datetime
import threading

import pytz
import requests
from flask import Flask, render_template, request, Response
from google.cloud import bigquery

import helpers

TABLE_NAMES = {
    'teams': 'commander.teams',
    'projections': 'commander.projections',
    'scores': 'commander.scores',
    'changes': 'commander.changes',
}

app = Flask(__name__)


@app.route("/update/all", methods=['GET'])
def update_all():
    """ Update all but live scores """

    responses = []

    responses.append(('projections', helpers.update_projections()))
    responses.append(('teams', helpers.update_teams()))
    update_scores()

    response = ', '.join(f"{key}: {value}" for key, value in responses)

    return Response(response, status=200 if False not in [r[1] for r in responses] else 500)


@app.route("/update/scores", methods=['GET'])
def update_scores():
    helpers.update_progress()
    helpers.update_all_scores()
    return Response('Success', 200)


@app.route("/changes", methods=['GET'])
def list_changes():

    changes = []

    for change in helpers.run_query(f"SELECT * FROM `{TABLE_NAMES.get('changes')}` ORDER BY updated DESC LIMIT 20", as_list=True):
        change = dict(change)
        change['diff'] = f"<span class='change-{'negative' if change.get('old') > change.get('new') else 'positive'}'>" \
                         f"{'-' if change.get('old') > change.get('new') else '+'}{abs(change.get('old') - change.get('new'))}</span>"
        changes.append(change)

    return render_template('changes.html', changes=changes)


@app.route("/records", methods=['GET'])
def records():

    leagues = []
    records = {}
    data = {}

    profiles = helpers.load_profiles()

    for league_list in profiles.values():
        
        for league in league_list:

            league_data = {
                'name': league.get('name'),
                'id': league.get('league_id'),
                'platform': league.get('platform'),
                'start': league.get('start_year')
            }
            
            if league_data not in leagues:
                leagues.append(league_data)

    threads = []

    for league in leagues:

        thread = threading.Thread(target=helpers.get_league_data, args=(data, league))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

    for league_name, league_data in data.items():

        if not league_data:
            continue

        records[league_name] = {
            'Highest Points (Week)': sorted(league_data, key=lambda x: x[4], reverse=True)[0:3],
            'Lowest Points (Week)': sorted(league_data, key=lambda x: x[4])[0:3],
            'Highest Projected (Week)': sorted(league_data, key=lambda x: x[5], reverse=True)[0:3],
            'Lowest Projected (Week)': sorted(league_data, key=lambda x: x[5])[0:3],
            'Best Outcome (Week)': sorted(league_data, key=lambda x: x[6], reverse=True)[0:3],
            'Worst Outcome (Week)': sorted(league_data, key=lambda x: x[6])[0:3],
        }

    return render_template('records.html', records=records)


@app.route("/", methods=['GET'])
def index():
    return ""


@app.route("/<string:profile>/<string:mode>", methods=['GET'])
def index_mode(profile: str, mode: str):
    return index_profile(profile, mode)


@app.route("/<string:profile>/", methods=['GET'])
def index_profile(profile: str, mode: str = 'default'):

    week = int(request.args.get('week')) if 'week' in request.args.keys() else helpers.get_current_week()
    matchups = helpers.get_all_matchups(profile, week, mode)

    return render_template('leagues.html', matchups=matchups, week=week)


if __name__ == '__main__':
    app.run()
