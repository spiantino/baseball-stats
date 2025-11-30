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
            print(">> Invalidated existing cache")
            print()

    # Prepare pitcher info
    pitcher_names = None
    pitcher_ids = None

    if args.away_pitcher or args.home_pitcher:
        pitcher_names = {}
        pitcher_ids = {}

        if args.away_pitcher:
            pitcher_names['away'] = args.away_pitcher
        if args.home_pitcher:
            pitcher_names['home'] = args.home_pitcher
        if args.away_pitcher_id:
            pitcher_ids['away'] = args.away_pitcher_id
        if args.home_pitcher_id:
            pitcher_ids['home'] = args.home_pitcher_id

    # Fetch data
    print("Fetching game data from all sources...")
    print("(This may take 30-60 seconds for all API calls)")
    print()

    try:
        data = fetch_game_data(
            away_team=args.away_team,
            home_team=args.home_team,
            game_date=args.game_date,
            season=args.season,
            use_cache=False,  # Always fetch fresh in this workflow
            pitcher_names=pitcher_names,
            pitcher_ids=pitcher_ids
        )

        print()
        print("="*60)
        print("FETCH COMPLETE!")
        print("="*60)
        print()

        # Show what was fetched
        print(f"+ Game info: {data['away_team_full']} @ {data['home_team_full']}")
        print(f"  Venue: {data.get('venue', 'TBD')}")
        print(f"  Time: {data.get('game_time', 'TBD')}")
        print()

        if 'away_pitcher' in data:
            pitcher = data['away_pitcher']
            print(f"+ {args.away_team} pitcher: {pitcher['name']}")
            print(f"  {pitcher['wins']}-{pitcher['losses']}, {pitcher['ERA']:.2f} ERA")

        if 'away_pitcher_pitches' in data:
            print(f"  Pitch mix: {len(data['away_pitcher_pitches'])} pitch types")

        if 'home_pitcher' in data:
            pitcher = data['home_pitcher']
            print(f"+ {args.home_team} pitcher: {pitcher['name']}")
            print(f"  {pitcher['wins']}-{pitcher['losses']}, {pitcher['ERA']:.2f} ERA")

        if 'home_pitcher_pitches' in data:
            print(f"  Pitch mix: {len(data['home_pitcher_pitches'])} pitch types")

        if 'away_lineup' in data:
            print(f"+ {args.away_team} lineup: {len(data['away_lineup'])} batters")

        if 'home_lineup' in data:
            print(f"+ {args.home_team} lineup: {len(data['home_lineup'])} batters")

        if 'division_race_data' in data:
            for division, teams in data['division_race_data'].items():
                print(f"+ {division} division race: {len(teams)} teams")

        print()

        # Show cache info
        cache_path = cache.get_cache_path(args.away_team, args.home_team, args.game_date)
        print(f">> Cached to: {cache_path}")
        print(f"   Size: {cache_path.stat().st_size / 1024:.1f} KB")
        print()

        print("Next step: Generate preview using cached data")
        print(f"  python scripts/generate_preview.py {args.away_team} {args.home_team} {args.game_date}")
        print()

    except Exception as e:
        logger.error(f"Failed to fetch game data: {e}", exc_info=True)
        print()
        print(f">> Error: {e}")
        print()
        sys.exit(1)


if __name__ == '__main__':
    main()
