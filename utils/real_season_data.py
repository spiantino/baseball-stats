"""
Fetch real MLB season data for division race charts.
"""

import pandas as pd
from typing import Dict
from datetime import datetime
from ingestion.mlb_api_client import MLBStatsAPIClient
from config.logging_config import get_logger

logger = get_logger(__name__)


def fetch_division_race_data(team_abbr: str, season: int = 2025) -> pd.DataFrame:
    """
    Fetch real game-by-game data for a team's season.

    Args:
        team_abbr: Team abbreviation (e.g., 'NYY')
        season: Season year (default: 2025)

    Returns:
        DataFrame with columns: game_number, wins, losses, games_above_500, result
    """
    client = MLBStatsAPIClient()

    # Fetch full season schedule
    start_date = f"{season}-03-01"
    end_date = f"{season}-10-31"

    logger.info(f"Fetching {season} season data for {team_abbr}")
    schedule = client.get_schedule(
        team=team_abbr,
        start_date=start_date,
        end_date=end_date
    )

    if not schedule:
        logger.warning(f"No schedule data found for {team_abbr} in {season}")
        return pd.DataFrame()

    # Process game results
    games = []
    wins = 0
    losses = 0
    game_number = 0

    for game in schedule:
        # Skip games that haven't been played yet
        if game.get('game_type') != 'R':  # Only regular season
            continue

        status = game.get('status', '')
        if status != 'Final' and status != 'Completed Early':
            continue

        game_number += 1

        # Determine if team won
        away_score = game.get('away_score', 0)
        home_score = game.get('home_score', 0)
        is_home = game.get('home_name', '').startswith(team_abbr) or game.get('home_id') == client._get_team_id(team_abbr)

        if is_home:
            won = home_score > away_score
        else:
            won = away_score > home_score

        if won:
            wins += 1
            result = 'W'
        else:
            losses += 1
            result = 'L'

        games_above_500 = wins - losses

        games.append({
            'game_number': game_number,
            'wins': wins,
            'losses': losses,
            'games_above_500': games_above_500,
            'result': result,
            'date': game.get('game_date', '')
        })

    df = pd.DataFrame(games)
    logger.info(f"{team_abbr}: {wins}-{losses} ({games_above_500:+d} games above .500)")

    return df


def fetch_division_teams_data(division_teams: list, season: int = 2025) -> Dict[str, pd.DataFrame]:
    """
    Fetch division race data for multiple teams.

    Args:
        division_teams: List of team abbreviations
        season: Season year (default: 2025)

    Returns:
        Dict mapping team abbr -> DataFrame with game-by-game records
    """
    team_data = {}

    for team in division_teams:
        df = fetch_division_race_data(team, season)
        if not df.empty:
            team_data[team] = df

    return team_data
