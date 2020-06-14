cd ~/workspaces/python/gg-ez

kedro run --pipeline fetch-player-stats --params league_id:775  # La Liga 2019-2020
kedro run --pipeline fetch-player-stats --params league_id:754  # Bundesliga 2019-2020
kedro run --pipeline fetch-player-stats --params league_id:524  # Premier 2019-2020
kedro run --pipeline fetch-player-stats --params league_id:891  # Serie A 2019-2020
kedro run --pipeline fetch-player-stats --params league_id:525  # Ligue 1 2019-2020
kedro run --pipeline fetch-player-stats --params league_id:766  # Primeira Liga 2019-2020