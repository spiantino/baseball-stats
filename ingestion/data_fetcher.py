"""
Data fetcher orchestrator for game preview data.

Coordinates fetching from multiple sources (MLB API, FanGraphs, Statcast)
and integrates with cache layer for efficient data management.
"""

from typing import Dict, Any, Optional
from ingestion.mlb_api_client import MLBStatsAPIClient
from ingestion.pybaseball_client import get_pitcher_stats, get_pitch_mix, get_batter_stats
from utils.team_data import get_team_full_name, get_team_logo_url, get_player_headshot_url
from utils.real_season_data import fetch_division_teams_data
from utils.data_cache import DataCache
from config.logging_config import get_logger

logger = get_logger(__name__)


def get_division_from_team(team_abbr: str) -> str:
    """Get division name for a team."""
    divisions = {
        # AL East
        'NYY': 'AL East', 'BOS': 'AL East', 'TB': 'AL East',
        'TOR': 'AL East', 'BAL': 'AL East',

        # AL Central
        'CLE': 'AL Central', 'MIN': 'AL Central', 'CWS': 'AL Central',
        'DET': 'AL Central', 'KC': 'AL Central',

        # AL West
        'HOU': 'AL West', 'TEX': 'AL West', 'SEA': 'AL West',
        'LAA': 'AL West', 'OAK': 'AL West',

        # NL East
        'ATL': 'NL East', 'PHI': 'NL East', 'NYM': 'NL East',
        'MIA': 'NL East', 'WSH': 'NL East',

        # NL Central
        'MIL': 'NL Central', 'STL': 'NL Central', 'CHC': 'NL Central',
        'CIN': 'NL Central', 'PIT': 'NL Central',

        # NL West
        'LAD': 'NL West', 'SD': 'NL West', 'SF': 'NL West',
        'COL': 'NL West', 'ARI': 'NL West',
    }
    return divisions.get(team_abbr, 'Unknown')


def get_division_teams(division: str) -> list[str]:
    """Get all teams in a division."""
    division_teams = {
        'AL East': ['NYY', 'BOS', 'TB', 'TOR', 'BAL'],
        'AL Central': ['CLE', 'MIN', 'CWS', 'DET', 'KC'],
        'AL West': ['HOU', 'TEX', 'SEA', 'LAA', 'OAK'],
        'NL East': ['ATL', 'PHI', 'NYM', 'MIA', 'WSH'],
        'NL Central': ['MIL', 'STL', 'CHC', 'CIN', 'PIT'],
        'NL West': ['LAD', 'SD', 'SF', 'COL', 'ARI'],
    }
    return division_teams.get(division, [])


def fetch_game_data(
    away_team: str,
    home_team: str,
    game_date: str,
    season: int = 2025,
    use_cache: bool = True,
    pitcher_names: Optional[Dict[str, str]] = None,
    pitcher_ids: Optional[Dict[str, int]] = None,
    lineups: Optional[Dict[str, list]] = None
) -> Dict[str, Any]:
    """
    Fetch complete game preview data from all sources.

    Args:
        away_team: Away team abbreviation (e.g., 'NYY')
        home_team: Home team abbreviation (e.g., 'BOS')
        game_date: Game date in YYYY-MM-DD format
        season: Season year (default: 2025)
        use_cache: Check cache before fetching (default: True)
        pitcher_names: Optional dict with 'away' and 'home' pitcher names
        pitcher_ids: Optional dict with 'away' and 'home' MLB player IDs
        lineups: Optional dict with 'away' and 'home' lineup lists

    Returns:
        Complete game data dict ready for template rendering
    """
    cache = DataCache()

    # Check cache first
    if use_cache:
        cached_data = cache.get(away_team, home_team, game_date)
        if cached_data:
            logger.info("Using cached data")
            return cached_data

    logger.info(f"Fetching fresh data for {away_team} @ {home_team} on {game_date}")

    # 1. Basic game info
    data = {
        'away_team': away_team,
        'home_team': home_team,
        'away_team_full': get_team_full_name(away_team),
        'home_team_full': get_team_full_name(home_team),
        'away_team_logo': get_team_logo_url(away_team),
        'home_team_logo': get_team_logo_url(home_team),
        'game_date': game_date,
        'away_division': get_division_from_team(away_team),
        'home_division': get_division_from_team(home_team),
    }

    # 2. Fetch game details from MLB API
    logger.info("Fetching game details from MLB API...")
    try:
        client = MLBStatsAPIClient()
        schedule = client.get_schedule(team=home_team, start_date=game_date, end_date=game_date)

        if schedule:
            game = schedule[0]
            data['game_time'] = game.get('game_time', 'TBD')
            data['venue'] = game.get('venue_name', 'Unknown Venue')
            data['away_record'] = f"{game.get('away_wins', 0)}-{game.get('away_losses', 0)}"
            data['home_record'] = f"{game.get('home_wins', 0)}-{game.get('home_losses', 0)}"
    except Exception as e:
        logger.warning(f"Could not fetch game details: {e}")
        data['game_time'] = 'TBD'
        data['venue'] = 'TBD'

    # 3. Fetch pitcher stats if names provided
    if pitcher_names:
        logger.info("Fetching pitcher stats from FanGraphs/Statcast...")

        if pitcher_names.get('away'):
            away_pitcher_stats = get_pitcher_stats(pitcher_names['away'], season)
            away_pitcher_pitches = get_pitch_mix(pitcher_names['away'], season)

            if away_pitcher_stats:
                away_pitcher_stats['number'] = 0  # Would need roster API
                if pitcher_ids and pitcher_ids.get('away'):
                    away_pitcher_stats['headshot_url'] = get_player_headshot_url(pitcher_ids['away'])
                data['away_pitcher'] = away_pitcher_stats

            if away_pitcher_pitches:
                data['away_pitcher_pitches'] = away_pitcher_pitches

        if pitcher_names.get('home'):
            home_pitcher_stats = get_pitcher_stats(pitcher_names['home'], season)
            home_pitcher_pitches = get_pitch_mix(pitcher_names['home'], season)

            if home_pitcher_stats:
                home_pitcher_stats['number'] = 0
                if pitcher_ids and pitcher_ids.get('home'):
                    home_pitcher_stats['headshot_url'] = get_player_headshot_url(pitcher_ids['home'])
                data['home_pitcher'] = home_pitcher_stats

            if home_pitcher_pitches:
                data['home_pitcher_pitches'] = home_pitcher_pitches

    # 4. Fetch lineup stats if lineups provided
    if lineups:
        logger.info("Fetching lineup stats from FanGraphs...")

        if lineups.get('away'):
            away_lineup = []
            for name, position, number in lineups['away']:
                batter_stats = get_batter_stats(name, season)
                if batter_stats:
                    batter_stats['position'] = position
                    batter_stats['number'] = number
                    away_lineup.append(batter_stats)

            if away_lineup:
                data['away_lineup'] = away_lineup

        if lineups.get('home'):
            home_lineup = []
            for name, position, number in lineups['home']:
                batter_stats = get_batter_stats(name, season)
                if batter_stats:
                    batter_stats['position'] = position
                    batter_stats['number'] = number
                    home_lineup.append(batter_stats)

            if home_lineup:
                data['home_lineup'] = home_lineup

    # 5. Fetch division race data
    logger.info("Fetching division race data from MLB API...")
    away_division = data['away_division']
    home_division = data['home_division']

    # Fetch away team's division
    away_division_teams = get_division_teams(away_division)
    away_division_data = fetch_division_teams_data(away_division_teams, season)

    if away_division_data:
        # Convert DataFrames to dict for JSON serialization
        serializable_data = {}
        for team, df in away_division_data.items():
            serializable_data[team] = df.to_dict(orient='records')
        data['division_race_data'] = {away_division: serializable_data}

    # Fetch home team's division if different
    if home_division != away_division:
        home_division_teams = get_division_teams(home_division)
        home_division_data = fetch_division_teams_data(home_division_teams, season)

        if home_division_data:
            # Convert DataFrames to dict for JSON serialization
            serializable_data = {}
            for team, df in home_division_data.items():
                serializable_data[team] = df.to_dict(orient='records')

            if 'division_race_data' not in data:
                data['division_race_data'] = {}
            data['division_race_data'][home_division] = serializable_data

    # Cache the data
    cache.set(away_team, home_team, game_date, data)

    return data
