"""
RE24 (Run Expectancy based on 24 base-out states) calculator.

Calculates RE24 for batters and pitchers from Statcast pitch-by-pitch data.
RE24 measures how much a player changed run expectancy during their plate appearances.

Statcast provides delta_run_exp per pitch, so we sum those per PA to get RE24.
"""

from typing import Dict, List, Optional, Any
from datetime import date
import pandas as pd
from pybaseball import statcast, statcast_batter
from config.logging_config import get_logger
from utils.api_cache import get_api_cache

logger = get_logger(__name__)

# Get shared cache instance
_cache = get_api_cache()


def get_game_re24(
    game_date: str,
    team: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate RE24 for all batters in games on a given date.

    Args:
        game_date: Date in YYYY-MM-DD format
        team: Optional team abbreviation to filter (e.g., 'NYY')

    Returns:
        Dict with:
            - games: List of game summaries with batter RE24
            - by_batter: Dict mapping batter_id -> RE24 totals
    """
    logger.info(f"Fetching Statcast data for {game_date}")

    try:
        df = statcast(game_date, game_date)
    except Exception as e:
        logger.error(f"Failed to fetch Statcast data: {e}")
        return {'games': [], 'by_batter': {}}

    if df.empty:
        logger.warning(f"No Statcast data for {game_date}")
        return {'games': [], 'by_batter': {}}

    logger.info(f"Processing {len(df)} pitches from {df['game_pk'].nunique()} games")

    # Filter out rows with null delta_run_exp (automatic balls/strikes)
    df = df[df['delta_run_exp'].notna()].copy()

    # Calculate RE24 per plate appearance
    # Group by game, at-bat number, and batter
    pa_re24 = df.groupby(['game_pk', 'at_bat_number', 'batter']).agg({
        'delta_run_exp': 'sum',
        'events': 'last',
        'bat_score': 'first',
        'post_bat_score': 'last',
        'home_team': 'first',
        'away_team': 'first',
        'inning_topbot': 'first'
    }).reset_index()

    pa_re24.rename(columns={'delta_run_exp': 're24'}, inplace=True)
    pa_re24['runs_scored'] = pa_re24['post_bat_score'] - pa_re24['bat_score']

    # Determine which team the batter is on
    pa_re24['batter_team'] = pa_re24.apply(
        lambda row: row['away_team'] if row['inning_topbot'] == 'Top' else row['home_team'],
        axis=1
    )

    # Filter by team if specified
    if team:
        pa_re24 = pa_re24[pa_re24['batter_team'] == team]

    # Aggregate by batter within each game
    batter_game_re24 = pa_re24.groupby(['game_pk', 'batter', 'batter_team', 'home_team', 'away_team']).agg({
        're24': 'sum',
        'runs_scored': 'sum',
        'at_bat_number': 'count'
    }).rename(columns={'at_bat_number': 'pa'}).reset_index()

    # Build result structure
    games = {}
    for _, row in batter_game_re24.iterrows():
        game_pk = row['game_pk']
        if game_pk not in games:
            games[game_pk] = {
                'game_pk': game_pk,
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'batters': []
            }

        games[game_pk]['batters'].append({
            'batter_id': int(row['batter']),
            'team': row['batter_team'],
            're24': round(row['re24'], 3),
            'pa': int(row['pa']),
            'runs_scored': int(row['runs_scored'])
        })

    # Sort batters by RE24 within each game
    for game in games.values():
        game['batters'].sort(key=lambda x: x['re24'], reverse=True)

    # Aggregate across all games by batter
    by_batter = batter_game_re24.groupby('batter').agg({
        're24': 'sum',
        'pa': 'sum',
        'runs_scored': 'sum'
    }).to_dict('index')

    by_batter = {
        int(k): {
            're24': round(v['re24'], 3),
            'pa': int(v['pa']),
            'runs_scored': int(v['runs_scored'])
        }
        for k, v in by_batter.items()
    }

    return {
        'games': list(games.values()),
        'by_batter': by_batter
    }


def get_batter_season_re24(
    batter_id: int,
    season: int = 2024,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get cumulative RE24 data for a batter over a season.

    Returns data formatted for the RE24 chart (game_number, cumulative_re24).

    Args:
        batter_id: MLB player ID
        season: Season year (used if start/end dates not provided)
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Dict with:
            - total_re24: Season total
            - pa: Total plate appearances
            - games: List of dicts with game_number, game_date, re24, cumulative_re24
    """
    if not start_date:
        start_date = f"{season}-03-20"
    if not end_date:
        end_date = f"{season}-10-01"

    # Check cache first
    cache_params = {'batter_id': batter_id, 'start_date': start_date, 'end_date': end_date}
    cached = _cache.get('statcast', 're24_season', cache_params)
    if cached is not None:
        logger.debug(f"Cache hit for RE24 season data: batter {batter_id}")
        return cached

    logger.info(f"Fetching season RE24 for batter {batter_id} from {start_date} to {end_date}")

    try:
        df = statcast_batter(start_date, end_date, batter_id)
    except Exception as e:
        logger.error(f"Failed to fetch batter Statcast data: {e}")
        return {'total_re24': 0, 'pa': 0, 'games': []}

    if df.empty:
        return {'total_re24': 0, 'pa': 0, 'games': []}

    # Filter nulls
    df = df[df['delta_run_exp'].notna()].copy()

    # Group by game and PA
    pa_re24 = df.groupby(['game_pk', 'game_date', 'at_bat_number']).agg({
        'delta_run_exp': 'sum',
        'events': 'last'
    }).reset_index()
    pa_re24.rename(columns={'delta_run_exp': 're24'}, inplace=True)

    # Aggregate by game
    game_re24 = pa_re24.groupby(['game_pk', 'game_date']).agg({
        're24': 'sum',
        'at_bat_number': 'count'
    }).rename(columns={'at_bat_number': 'pa'}).reset_index()

    # Sort by date and add game numbers
    game_re24['game_date'] = pd.to_datetime(game_re24['game_date'])
    game_re24 = game_re24.sort_values('game_date')
    game_re24['game_number'] = range(1, len(game_re24) + 1)
    game_re24['cumulative_re24'] = game_re24['re24'].cumsum()

    # Format for output
    game_re24['game_date'] = game_re24['game_date'].dt.strftime('%Y-%m-%d')

    games = []
    for _, row in game_re24.iterrows():
        games.append({
            'game_number': int(row['game_number']),
            'game_date': row['game_date'],
            're24': round(row['re24'], 3),
            'cumulative_re24': round(row['cumulative_re24'], 3),
            'pa': int(row['pa'])
        })

    result = {
        'total_re24': round(game_re24['re24'].sum(), 3),
        'pa': int(game_re24['pa'].sum()),
        'games': games
    }

    # Cache the result
    _cache.set('statcast', 're24_season', cache_params, result)

    return result


def get_team_re24_data(
    player_ids: List[int],
    player_names: Dict[int, str],
    season: int = 2024,
    end_date: Optional[str] = None
) -> Dict[str, List[Dict]]:
    """
    Get cumulative RE24 data for multiple players on a team.

    Returns data formatted for create_re24_chart().

    Args:
        player_ids: List of MLB player IDs
        player_names: Dict mapping player_id -> display name
        season: Season year
        end_date: Optional end date (defaults to Oct 1)

    Returns:
        Dict mapping player name -> list of game records with game_number, cumulative_re24
    """
    result = {}

    for player_id in player_ids:
        name = player_names.get(player_id, str(player_id))
        logger.info(f"Fetching RE24 for {name} (ID: {player_id})")

        data = get_batter_season_re24(player_id, season, end_date=end_date)

        if data['games']:
            result[name] = data['games']
            logger.info(f"  {name}: {data['total_re24']:+.1f} RE24 over {len(data['games'])} games")
        else:
            logger.warning(f"  No RE24 data for {name}")

    return result


# Legacy function name for compatibility
def get_season_re24(
    batter_id: int,
    season: int = 2024,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """Legacy wrapper for get_batter_season_re24."""
    return get_batter_season_re24(batter_id, season, start_date, end_date)


if __name__ == '__main__':
    # Test the calculator
    import sys
    sys.path.insert(0, '.')

    result = get_game_re24('2024-09-25')
    print(f"Found {len(result['games'])} games")

    if result['games']:
        game = result['games'][0]
        print(f"\nGame: {game['away_team']} @ {game['home_team']}")
        print("Top 5 batters by RE24:")
        for b in game['batters'][:5]:
            print(f"  {b['batter_id']}: {b['re24']:+.3f} ({b['pa']} PA)")
