#!/usr/bin/env python3
"""
Workflow A: Fetch and cache game data.

Fetches all data from MLB API, FanGraphs, and Statcast and caches it as JSON.
This is the SLOW operation that hits all APIs. Run once per game.

Usage:
    python scripts/fetch_game_data.py NYY BOS 2025-09-25 \\
        --away-pitcher "Marcus Stroman" \\
        --home-pitcher "Brayan Bello" \\
        --away-pitcher-id 573186 \\
        --home-pitcher-id 676656
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.data_fetcher import fetch_game_data
from utils.data_cache import DataCache
from config.logging_config import get_logger

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Fetch and cache game preview data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic fetch (no pitcher/lineup data)
  python scripts/fetch_game_data.py NYY BOS 2025-09-25

  # With pitcher info
  python scripts/fetch_game_data.py NYY BOS 2025-09-25 \\
      --away-pitcher "Marcus Stroman" \\
      --home-pitcher "Brayan Bello" \\
      --away-pitcher-id 573186 \\
      --home-pitcher-id 676656

  # Force refresh (invalidate cache first)
  python scripts/fetch_game_data.py NYY BOS 2025-09-25 --force
        """
    )

    parser.add_argument('away_team', help='Away team abbreviation (e.g., NYY)')
    parser.add_argument('home_team', help='Home team abbreviation (e.g., BOS)')
    parser.add_argument('game_date', help='Game date (YYYY-MM-DD)')
    parser.add_argument('--season', type=int, default=2025, help='Season year (default: 2025)')

    # Pitcher arguments
    parser.add_argument('--away-pitcher', help='Away pitcher name (e.g., "Marcus Stroman")')
    parser.add_argument('--home-pitcher', help='Home pitcher name')
    parser.add_argument('--away-pitcher-id', type=int, help='Away pitcher MLB player ID')
    parser.add_argument('--home-pitcher-id', type=int, help='Home pitcher MLB player ID')

    # Lineup arguments (for future use)
    parser.add_argument('--away-lineup-file', help='JSON file with away lineup')
    parser.add_argument('--home-lineup-file', help='JSON file with home lineup')

    # Cache control
    parser.add_argument('--force', action='store_true', help='Force refresh (invalidate cache)')

    args = parser.parse_args()

    print()
    print("="*60)
    print("FETCH GAME DATA (Workflow A)")
    print("="*60)
    print()
    print(f"Game: {args.away_team} @ {args.home_team}")
    print(f"Date: {args.game_date}")
    print(f"Season: {args.season}")
    print()

    # Handle force refresh
    cache = DataCache()
    if args.force:
        if cache.has_cache(args.away_team, args.home_team, args.game_date):
            cache.invalidate(args.away_team, args.home_team, args.game_date)
