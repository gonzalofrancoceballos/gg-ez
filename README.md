# gg-ez
Data fetching, consolidation, processing and analytics to became the next GOD in fantasy football

# Pipelines
## `fetch` pipelines
Pull data from API directly into ``01_raw`` folder without changing original format
- `fetch-leagues`: fetches all available leagues in the API
- `fetch-all-games`: fetches game info for all available leagues
- `fetch-player-stats`: fetches player stats by game for a league defined in`params:league_id` that have not been fetched yet

## `pre-process` pipelines
Pre-process raw data from ``01_raw`` into ``02_intermediate``, converting it to tabular format
- `pre-process-leagues`: pre-proceses all leagues into a single table
- `pre-process-games`: pre-processes all fetched game info into a consolidated table
- `pre-process-players`: pre-processes all player stats into a consolidated table

## ``process`` pipelines
Process information into final format