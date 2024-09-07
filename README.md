## Multi-League Fantasy Dashboard aka "Commander"

# Features

* Single page scoreboard for all of your matchup scores (you and opponent) across multiple leagues
* Supports ESPN and Sleeper leagues
* Supports multiple users in the same and/or different leagues
* Projections from FantasyPros based on per-league Standard, Half, and Full PPR settings
* Dynamic projections after the 1st quarter, extrapolated from current points to game time remaining
* Actively playing and gameday highlighting for players
* Questionable, Out, and IR designation outlining
* Monitor all projections for sharp changes to notify on (not fully implemented)

# Architecture

* Python
* Flask for the web framework
* Jinja, HTML, Grid CSS for frontend design
* BigQuery for DB backend

# Workflow

1. Update API endpoints get live league, team, matchup, score, projection, and game progress data
2. Store each group of data in BigQuery
3. Access the scoreboard for each profile
4. Scoreboard loads the cached data based on the league and team data for your profile

# Disclaimers

* This was built for the 2023 season, so some things may not fully work yet for 2024. I've updated some code so it appears to work but no guarantees.
* Pulling data for multiple leagues can be slow. I'm using BigQuery to cache so it doesn't need to pull fully live data, but it can still take 5-10 seconds to load for several leagues.
* Suggestions to improve speed:
  * Try a beefy Cloud Run instance with more CPU
  * Switch to caching to a local sqlite3 database for faster DB reads

# Setup

Here's the easiest way to setup and also the way I use it:

1. Create a free Google Cloud Platform account and create a new project
2. Create a dataset named commander with the following tables (get the schemas from the code):
   1. leagues - stores league and profile data
   2. teams - stores team data for each league (names, owners, IDs, etc)
   3. scores - current points
   4. projections - current projections
   5. matchups - current matchups
   6. game_progress - the time remaining for each game, for dynamic projections
   7. changes - for monitoring quick projection changes (not fully implemented)
3. Create a Cloud Run service, set to continuously deploy from this repo (or a fork) using Dockerfile
4. This should deploy the service; you can then check the URL from the Cloud Run instance page
5. Go to the URL from above and add /update/all to the end to update all leagues, teams, scores, and projections
6. Set up scheduled tasks from Cloud Scheduler to /update/scores for every few minutes ONLY during gametimes to save processing and /update/all for every hour or so

That should be everything. You can then access the scoreboard using the instance URL and add /profile_name to the end.
