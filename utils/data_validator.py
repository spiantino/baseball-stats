"""
Data validation module for game preview data.

Validates that all required data is present before rendering preview.
Fails loudly with clear error messages when data is missing.
"""

from typing import Dict, List, Any
from config.logging_config import get_logger

logger = get_logger(__name__)


class DataValidationError(Exception):
    """Raised when required data is missing or invalid."""
    pass


def validate_game_data(data: Dict[str, Any], strict: bool = True) -> Dict[str, List[str]]:
    """
    Validate game preview data completeness.

    Args:
        data: Game data dict to validate
        strict: If True, raise exception on missing required data

    Returns:
        Dict with 'missing' and 'warnings' lists

    Raises:
        DataValidationError: If strict=True and required data is missing
    """
    missing = []
    warnings = []

    # Required basic game info
    required_fields = [
        'away_team', 'home_team', 'game_date',
        'away_team_full', 'home_team_full',
        'venue', 'game_time'
    ]

    for field in required_fields:
        if field not in data or not data[field]:
            missing.append(f"Basic game info: {field}")

    # Team records
    if 'away_record' not in data or data['away_record'] == '0-0':
        warnings.append("Away team record missing or zero")
    if 'home_record' not in data or data['home_record'] == '0-0':
        warnings.append("Home team record missing or zero")

    # Starting pitchers (critical)
    if 'away_pitcher' not in data:
        missing.append("Away starting pitcher stats")
    else:
        _validate_pitcher_stats(data['away_pitcher'], 'away', missing, warnings)

    if 'home_pitcher' not in data:
        missing.append("Home starting pitcher stats")
    else:
        _validate_pitcher_stats(data['home_pitcher'], 'home', missing, warnings)

    # Pitch mix (important but not critical)
    if 'away_pitcher_pitches' not in data:
        warnings.append("Away pitcher pitch mix missing")
    if 'home_pitcher_pitches' not in data:
        warnings.append("Home pitcher pitch mix missing")

    # Lineups (critical)
    if 'away_lineup' not in data:
        missing.append("Away team lineup")
    elif len(data['away_lineup']) < 8:
        warnings.append(f"Away lineup incomplete ({len(data['away_lineup'])}/9 batters)")

    if 'home_lineup' not in data:
        missing.append("Home team lineup")
    elif len(data['home_lineup']) < 8:
        warnings.append(f"Home lineup incomplete ({len(data['home_lineup'])}/9 batters)")

    # Bench (nice to have)
    if 'away_bench' not in data:
        warnings.append("Away bench players missing")
    if 'home_bench' not in data:
        warnings.append("Home bench players missing")

    # Bullpen (nice to have)
    if 'away_bullpen' not in data:
        warnings.append("Away bullpen missing")
    if 'home_bullpen' not in data:
        warnings.append("Home bullpen missing")

    # Division race (nice to have)
    if 'division_race_data' not in data:
        warnings.append("Division race data missing")

    # Log results
    if missing:
        logger.error(f"MISSING REQUIRED DATA ({len(missing)} items):")
        for item in missing:
            logger.error(f"  ✗ {item}")

    if warnings:
        logger.warning(f"Data warnings ({len(warnings)} items):")
        for item in warnings:
            logger.warning(f"  ⚠ {item}")

    # Raise exception in strict mode
    if strict and missing:
        error_msg = f"Missing {len(missing)} required data fields:\n" + "\n".join(f"  - {m}" for m in missing)
        raise DataValidationError(error_msg)

    return {
        'missing': missing,
        'warnings': warnings
    }


def _validate_pitcher_stats(pitcher: Dict, team: str, missing: List, warnings: List) -> None:
    """Validate pitcher stats completeness."""
    required_stats = ['name', 'wins', 'losses', 'ERA', 'WHIP', 'K/9', 'BB/9', 'IP', 'WAR']

    for stat in required_stats:
        if stat not in pitcher or pitcher[stat] is None:
            missing.append(f"{team.capitalize()} pitcher: missing {stat}")

    # Check for zero/placeholder values
    if pitcher.get('IP', 0) == 0:
        warnings.append(f"{team.capitalize()} pitcher has 0 innings pitched")


def print_data_summary(data: Dict[str, Any]) -> None:
    """
    Print a summary of what data is available.

    Args:
        data: Game data dict
    """
    logger.info("=" * 60)
    logger.info("DATA AVAILABILITY SUMMARY")
    logger.info("=" * 60)

    # Basic info
    logger.info(f"Game: {data.get('away_team', '?')} @ {data.get('home_team', '?')}")
    logger.info(f"Date: {data.get('game_date', '?')}")
    logger.info(f"Venue: {data.get('venue', '?')}")
    logger.info(f"Time: {data.get('game_time', '?')}")

    # Pitchers
    logger.info(f"\nStarting Pitchers:")
    logger.info(f"  Away: {'✓' if 'away_pitcher' in data else '✗'} {data.get('away_pitcher', {}).get('name', 'N/A')}")
    logger.info(f"  Home: {'✓' if 'home_pitcher' in data else '✗'} {data.get('home_pitcher', {}).get('name', 'N/A')}")

    # Pitch mix
    logger.info(f"\nPitch Mix:")
    logger.info(f"  Away: {'✓' if 'away_pitcher_pitches' in data else '✗'} ({len(data.get('away_pitcher_pitches', []))} pitches)")
    logger.info(f"  Home: {'✓' if 'home_pitcher_pitches' in data else '✗'} ({len(data.get('home_pitcher_pitches', []))} pitches)")

    # Lineups
    logger.info(f"\nLineups:")
    logger.info(f"  Away: {'✓' if 'away_lineup' in data else '✗'} ({len(data.get('away_lineup', []))} batters)")
    logger.info(f"  Home: {'✓' if 'home_lineup' in data else '✗'} ({len(data.get('home_lineup', []))} batters)")

    # Bench
    logger.info(f"\nBench:")
    logger.info(f"  Away: {'✓' if 'away_bench' in data else '✗'} ({len(data.get('away_bench', []))} players)")
    logger.info(f"  Home: {'✓' if 'home_bench' in data else '✗'} ({len(data.get('home_bench', []))} players)")

    # Bullpen
    logger.info(f"\nBullpen:")
    logger.info(f"  Away: {'✓' if 'away_bullpen' in data else '✗'} ({len(data.get('away_bullpen', []))} pitchers)")
    logger.info(f"  Home: {'✓' if 'home_bullpen' in data else '✗'} ({len(data.get('home_bullpen', []))} pitchers)")

    # Division
    logger.info(f"\nDivision Race: {'✓' if 'division_race_data' in data else '✗'}")

    logger.info("=" * 60)
