"""
PyBaseball client for fetching real MLB stats from FanGraphs and Baseball Savant.
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from pybaseball import (
    pitching_stats,
    batting_stats,
    playerid_lookup,
    statcast_pitcher
)
from config.logging_config import get_logger

logger = get_logger(__name__)


def get_pitcher_stats(pitcher_name: str, season: int = 2025) -> Optional[Dict[str, Any]]:
    """
    Get pitcher stats from FanGraphs for a season.

    Args:
        pitcher_name: Pitcher's name (e.g., "Gerrit Cole")
        season: Season year

    Returns:
        Dict with pitcher stats or None
    """
    try:
        # Get all pitcher stats for the season
        logger.info(f"Fetching pitcher stats for {pitcher_name}, {season} season")
        stats = pitching_stats(season, season, qual=0)  # qual=0 gets all pitchers

        # Find the pitcher
        name_parts = pitcher_name.split()
        if len(name_parts) < 2:
            logger.error(f"Invalid pitcher name format: {pitcher_name}")
            return None

        first_name = name_parts[0]
        last_name = name_parts[-1]

        # Search for pitcher (case insensitive)
        pitcher_df = stats[
            (stats['Name'].str.contains(first_name, case=False, na=False)) &
            (stats['Name'].str.contains(last_name, case=False, na=False))
        ]

        if pitcher_df.empty:
            logger.warning(f"No stats found for {pitcher_name} in {season}")
            return None

        # Get first match
        pitcher = pitcher_df.iloc[0]

        return {
            'name': str(pitcher.get('Name', pitcher_name)),
            'hand': str(pitcher.get('Hand', 'R'))[0],  # R or L
            'WAR': float(pitcher.get('WAR', 0)),
            'wins': int(pitcher.get('W', 0)),
            'losses': int(pitcher.get('L', 0)),
            'ERA': float(pitcher.get('ERA', 0)),
            'IP': float(pitcher.get('IP', 0)),
            'K/9': float(pitcher.get('K/9', 0)),
            'BB/9': float(pitcher.get('BB/9', 0)),
            'HR/9': float(pitcher.get('HR/9', 0)),
            'WHIP': float(pitcher.get('WHIP', 0)),
            'GB%': float(pitcher.get('GB%', 0)) if pd.notna(pitcher.get('GB%')) else 45.0,
        }

    except Exception as e:
        logger.error(f"Error fetching pitcher stats for {pitcher_name}: {e}", exc_info=True)
        return None


def get_pitch_mix(pitcher_name: str, season: int = 2025) -> List[Dict[str, Any]]:
    """
    Get pitch mix data from Statcast/Baseball Savant.

    Args:
        pitcher_name: Pitcher's name
        season: Season year

    Returns:
        List of pitch dicts with name, usage, velocity, spin, whiff
    """
    try:
        logger.info(f"Fetching pitch mix for {pitcher_name}, {season} season")

        # Look up player ID
        name_parts = pitcher_name.split()
        if len(name_parts) < 2:
            return []

        first_name = name_parts[0]
        last_name = name_parts[-1]

        # Get player ID from lookup
        player_lookup = playerid_lookup(last_name, first_name)
        if player_lookup.empty:
            logger.warning(f"Could not find player ID for {pitcher_name}")
            return []

        mlb_id = player_lookup.iloc[0]['key_mlbam']

        # Get Statcast data
        statcast_data = statcast_pitcher(f'{season}-03-01', f'{season}-10-31', int(mlb_id))

        if statcast_data.empty:
            logger.warning(f"No Statcast data for {pitcher_name} in {season}")
            return []

        # Group by pitch type
        pitch_groups = statcast_data.groupby('pitch_type')

        pitch_mix = []
        total_pitches = len(statcast_data)

        for pitch_type, group in pitch_groups:
            if pd.isna(pitch_type) or pitch_type == '':
                continue

            count = len(group)
            usage = (count / total_pitches) * 100

            # Get pitch name
            pitch_name = get_pitch_name(pitch_type)

            # Calculate stats
            avg_velo = group['release_speed'].mean() if 'release_speed' in group else 0
            avg_spin = int(group['release_spin_rate'].mean()) if 'release_spin_rate' in group else 0

            # Whiff rate (swinging strikes / total swings)
            swings = group[group['description'].isin(['swinging_strike', 'swinging_strike_blocked', 'foul', 'hit_into_play'])]
            whiffs = group[group['description'].isin(['swinging_strike', 'swinging_strike_blocked'])]
            whiff_rate = (len(whiffs) / len(swings) * 100) if len(swings) > 0 else 0

            pitch_mix.append({
                'name': pitch_name,
                'usage': usage,
                'velocity': avg_velo,
                'spin': avg_spin,
                'whiff': whiff_rate
            })

        # Sort by usage
        pitch_mix.sort(key=lambda x: x['usage'], reverse=True)

        return pitch_mix

    except Exception as e:
        logger.error(f"Error fetching pitch mix for {pitcher_name}: {e}", exc_info=True)
        return []


def get_pitch_name(pitch_code: str) -> str:
    """Convert pitch type code to readable name."""
    pitch_names = {
        'FF': '4-Seam FB',
        'SI': 'Sinker',
        'FC': 'Cutter',
        'SL': 'Slider',
        'CU': 'Curveball',
        'CH': 'Changeup',
        'FS': 'Splitter',
        'KN': 'Knuckleball',
        'SC': 'Screwball',
        'FO': 'Forkball',
        'EP': 'Eephus',
        'FA': 'Fastball',
        'ST': 'Sweeper',
        'SV': 'Slurve'
    }
    return pitch_names.get(pitch_code, pitch_code)


def get_batter_stats(batter_name: str, season: int = 2025) -> Optional[Dict[str, Any]]:
    """
    Get batter stats from FanGraphs for a season.

    Args:
        batter_name: Batter's name (e.g., "Aaron Judge")
        season: Season year

    Returns:
        Dict with batter stats or None
    """
    try:
        logger.info(f"Fetching batter stats for {batter_name}, {season} season")
        stats = batting_stats(season, season, qual=0)  # Get all batters

        # Find the batter
        name_parts = batter_name.split()
        if len(name_parts) < 2:
            return None

        first_name = name_parts[0]
        last_name = name_parts[-1]

        batter_df = stats[
            (stats['Name'].str.contains(first_name, case=False, na=False)) &
            (stats['Name'].str.contains(last_name, case=False, na=False))
        ]

        if batter_df.empty:
            logger.warning(f"No stats found for {batter_name} in {season}")
            return None

        batter = batter_df.iloc[0]

        # Format slash line (AVG/OBP/SLG)
        avg = float(batter.get('AVG', 0))
        obp = float(batter.get('OBP', 0))
        slg = float(batter.get('SLG', 0))
        slash = f".{int(avg*1000):03d}/.{int(obp*1000):03d}/.{int(slg*1000):03d}"

        return {
            'name': str(batter.get('Name', batter_name)),
            'war': float(batter.get('WAR', 0)),
            'slash': slash,
            'hr': int(batter.get('HR', 0)),
            'rbi': int(batter.get('RBI', 0)),
            'sb': int(batter.get('SB', 0)),
            'owar': float(batter.get('Off', 0)),  # Offensive WAR
            'dwar': float(batter.get('Def', 0)),  # Defensive WAR
        }

    except Exception as e:
        logger.error(f"Error fetching batter stats for {batter_name}: {e}", exc_info=True)
        return None
