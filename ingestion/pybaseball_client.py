"""
PyBaseball client for fetching real MLB stats from Baseball Reference and Baseball Savant (Statcast).

Data Sources:
- Baseball Reference: Pitcher/batter traditional stats (ERA, AVG, WAR, etc.)
- Baseball Savant (Statcast): Pitch-by-pitch data for pitch mix and advanced metrics
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import unicodedata
from pybaseball import (
    pitching_stats,
    pitching_stats_bref,
    batting_stats,
    batting_stats_bref,
    playerid_lookup,
    statcast_pitcher,
    cache
)
from config.logging_config import get_logger
from utils.api_cache import get_api_cache

# Enable pybaseball caching for faster subsequent fetches
# Cache is stored in ~/.pybaseball/cache/
cache.enable()

logger = get_logger(__name__)

# Get shared API cache instance
_cache = get_api_cache()


def normalize_name(name: str) -> str:
    """
    Normalize player name by removing accents/diacritics.

    Converts: "Rodríguez" -> "Rodriguez", "José" -> "Jose"

    Args:
        name: Player name with possible accents

    Returns:
        Name with accents removed (ASCII)
    """
    # Normalize to NFD (decomposed form), then filter out combining characters
    nfd = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    return ascii_name


def get_pitcher_stats(pitcher_name: str, season: int = 2025) -> Optional[Dict[str, Any]]:
    """
    Get pitcher stats from Baseball Reference for a season.

    Args:
        pitcher_name: Pitcher's name (e.g., "Gerrit Cole")
        season: Season year

    Returns:
        Dict with pitcher stats or None
    """
    try:
        # Get all pitcher stats for the season from Baseball Reference
        logger.info(f"Fetching pitcher stats for {pitcher_name}, {season} season (Baseball Reference)")
        stats = pitching_stats_bref(season)

        # Find the pitcher
        name_parts = pitcher_name.split()
        if len(name_parts) < 2:
            logger.error(f"Invalid pitcher name format: {pitcher_name}")
            return None

        # Normalize name parts for matching (handles accented characters)
        first_name = name_parts[0]
        last_name = name_parts[-1]
        first_name_normalized = normalize_name(first_name)
        last_name_normalized = normalize_name(last_name)

        # Normalize the Name column in stats for matching
        stats['Name_normalized'] = stats['Name'].apply(normalize_name)

        # Search for pitcher (try normalized names first, then original)
        pitcher_df = stats[
            (stats['Name_normalized'].str.contains(first_name_normalized, case=False, na=False)) &
            (stats['Name_normalized'].str.contains(last_name_normalized, case=False, na=False))
        ]

        if pitcher_df.empty:
            logger.warning(f"No stats found for {pitcher_name} in {season} (tried normalized: {first_name_normalized} {last_name_normalized})")
            return None

        # Get first match
        pitcher = pitcher_df.iloc[0]

        # Calculate rate stats if not available
        ip = float(pitcher.get('IP', 0))
        so = int(pitcher.get('SO', 0))
        bb = int(pitcher.get('BB', 0))
        hr = int(pitcher.get('HR', 0))

        # K/9, BB/9, HR/9
        k_9 = (so / ip * 9) if ip > 0 else 0
        bb_9 = (bb / ip * 9) if ip > 0 else 0
        hr_9 = (hr / ip * 9) if ip > 0 else 0

        return {
            'name': str(pitcher.get('Name', pitcher_name)),
            'hand': str(pitcher.get('Hand', 'R'))[0] if pd.notna(pitcher.get('Hand')) else 'R',  # R or L
            'WAR': float(pitcher.get('WAR', 0)),
            'wins': int(pitcher.get('W', 0)),
            'losses': int(pitcher.get('L', 0)),
            'ERA': float(pitcher.get('ERA', 0)),
            'IP': ip,
            'K/9': k_9,
            'BB/9': bb_9,
            'HR/9': hr_9,
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
    # Check cache first
    cache_params = {'pitcher_name': pitcher_name, 'season': season}
    cached = _cache.get('statcast', 'pitch_mix', cache_params)
    if cached is not None:
        logger.debug(f"Cache hit for pitch mix: {pitcher_name}")
        return cached

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

            # RV/100 (run value per 100 pitches)
            # delta_run_exp is the run expectancy change on each pitch
            if 'delta_run_exp' in group.columns:
                run_value_per_pitch = group['delta_run_exp'].mean()
                rv_100 = run_value_per_pitch * 100
            else:
                rv_100 = 0

            pitch_mix.append({
                'name': pitch_name,
                'usage': usage,
                'velocity': avg_velo,
                'spin': avg_spin,
                'whiff': whiff_rate,
                'rv_100': rv_100
            })

        # Sort by usage
        pitch_mix.sort(key=lambda x: x['usage'], reverse=True)

        # Cache the result
        _cache.set('statcast', 'pitch_mix', cache_params, pitch_mix)

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
    Get batter stats from Baseball Reference for a season.

    Args:
        batter_name: Batter's name (e.g., "Aaron Judge")
        season: Season year

    Returns:
        Dict with batter stats or None
    """
    try:
        logger.info(f"Fetching batter stats for {batter_name}, {season} season (Baseball Reference)")
        stats = batting_stats_bref(season)

        # Find the batter
        name_parts = batter_name.split()
        if len(name_parts) < 2:
            return None

        # Normalize name parts for matching (handles accented characters)
        first_name = name_parts[0]
        last_name = name_parts[-1]
        first_name_normalized = normalize_name(first_name)
        last_name_normalized = normalize_name(last_name)

        # Normalize the Name column in stats for matching
        stats['Name_normalized'] = stats['Name'].apply(normalize_name)

        # Search for batter using normalized names
        batter_df = stats[
            (stats['Name_normalized'].str.contains(first_name_normalized, case=False, na=False)) &
            (stats['Name_normalized'].str.contains(last_name_normalized, case=False, na=False))
        ]

        if batter_df.empty:
            logger.warning(f"No stats found for {batter_name} in {season} (tried normalized: {first_name_normalized} {last_name_normalized})")
            return None

        batter = batter_df.iloc[0]

        # Format slash line (AVG/OBP/SLG)
        avg = float(batter.get('BA', 0))  # Baseball Reference uses 'BA' instead of 'AVG'
        obp = float(batter.get('OBP', 0))
        slg = float(batter.get('SLG', 0))
        slash = f".{int(avg*1000):03d}/.{int(obp*1000):03d}/.{int(slg*1000):03d}"

        # Calculate OPS
        ops = obp + slg

        # Get OPS+ (Baseball Reference has actual OPS+, not wRC+)
        ops_plus = int(batter.get('OPS+', 100))

        return {
            'name': str(batter.get('Name', batter_name)),
            'war': float(batter.get('WAR', 0)),
            'slash': slash,
            'ops': ops,
            'ops_plus': ops_plus,
            'hr': int(batter.get('HR', 0)),
            'rbi': int(batter.get('RBI', 0)),
            'sb': int(batter.get('SB', 0)),
            'owar': float(batter.get('oWAR', 0)) if pd.notna(batter.get('oWAR')) else 0.0,  # Offensive WAR
            'dwar': float(batter.get('dWAR', 0)) if pd.notna(batter.get('dWAR')) else 0.0,  # Defensive WAR
        }

    except Exception as e:
        logger.error(f"Error fetching batter stats for {batter_name}: {e}", exc_info=True)
        return None


def get_fangraphs_batting_stats(season: int = 2025) -> Optional[pd.DataFrame]:
    """
    Get all FanGraphs batting stats for a season.

    Returns DataFrame with oWAR, dWAR, wRC+, etc. for all qualified batters.
    Results are cached for the session.

    Args:
        season: Season year

    Returns:
        DataFrame with FanGraphs batting stats or None
    """
    # Check cache first
    cache_params = {'season': season}
    cached = _cache.get('fangraphs', 'batting_stats', cache_params)
    if cached is not None:
        logger.debug(f"Cache hit for FanGraphs batting stats {season}")
        return pd.DataFrame(cached)

    try:
        logger.info(f"Fetching FanGraphs batting stats for {season} season...")
        # qual=1 means at least 1 PA (we want all players)
        df = batting_stats(season, qual=1)
        logger.info(f"Got FanGraphs stats for {len(df)} batters")

        # Cache as list of dicts for JSON serialization
        _cache.set('fangraphs', 'batting_stats', cache_params, df.to_dict('records'))

        return df

    except Exception as e:
        logger.error(f"Error fetching FanGraphs batting stats: {e}", exc_info=True)
        return None


def get_batter_fangraphs_stats(player_name: str, season: int = 2025) -> Optional[Dict[str, Any]]:
    """
    Get batter stats from FanGraphs for a season.

    This provides oWAR, dWAR, wRC+ and other advanced stats.

    Args:
        player_name: Player's name (e.g., "Aaron Judge")
        season: Season year

    Returns:
        Dict with FanGraphs stats or None
    """
    try:
        # Get full FanGraphs dataset (cached)
        df = get_fangraphs_batting_stats(season)
        if df is None or df.empty:
            return None

        # Normalize player name for matching
        name_parts = player_name.split()
        if len(name_parts) < 2:
            return None

        first_name = name_parts[0]
        last_name = name_parts[-1]
        first_name_normalized = normalize_name(first_name)
        last_name_normalized = normalize_name(last_name)

        # Normalize names in dataframe
        df['Name_normalized'] = df['Name'].apply(normalize_name)

        # Search for player
        player_df = df[
            (df['Name_normalized'].str.contains(first_name_normalized, case=False, na=False)) &
            (df['Name_normalized'].str.contains(last_name_normalized, case=False, na=False))
        ]

        if player_df.empty:
            logger.debug(f"No FanGraphs stats for {player_name}")
            return None

        player = player_df.iloc[0]

        # FanGraphs columns:
        # WAR = total WAR
        # Off = offensive runs above average
        # Def = defensive runs above average
        # wRC+ = weighted runs created plus (like OPS+)

        # Calculate oWAR and dWAR from Off and Def
        # These are approximations - FanGraphs doesn't split WAR directly
        # But Off and Def are the run values that go into WAR
        total_war = float(player.get('WAR', 0))
        off_runs = float(player.get('Off', 0))
        def_runs = float(player.get('Def', 0))

        # Rough conversion: ~10 runs = 1 WAR
        # But we'll use the actual Off/Def values scaled
        if off_runs + def_runs != 0:
            off_share = off_runs / (abs(off_runs) + abs(def_runs)) if (abs(off_runs) + abs(def_runs)) > 0 else 0.5
            owar = total_war * off_share if off_runs >= 0 else min(0, total_war * off_share)
            dwar = total_war - owar
        else:
            owar = total_war / 2
            dwar = total_war / 2

        # Actually, let's just use Off and Def directly as run values
        # and convert to WAR-like scale (divide by 10)
        owar = off_runs / 10
        dwar = def_runs / 10

        return {
            'war': total_war,
            'owar': round(owar, 1),
            'dwar': round(dwar, 1),
            'wrc_plus': int(player.get('wRC+', 100)),
            'off': off_runs,
            'def': def_runs,
        }

    except Exception as e:
        logger.error(f"Error fetching FanGraphs stats for {player_name}: {e}", exc_info=True)
        return None
