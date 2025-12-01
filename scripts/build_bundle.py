#!/usr/bin/env python3
"""
Step 2: Build game bundle from cached API data.

Assembles all cached API responses into a single game bundle JSON file.
This step does NOT hit external APIs - it only reads from API cache.

Usage:
    python scripts/build_bundle.py CWS NYY 2025-09-25
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.data_fetcher import fetch_game_data
from ingestion.mlb_api_client import MLBStatsAPIClient
from utils.team_data import get_team_full_name
from config.logging_config import get_logger
import statsapi

logger = get_logger(__name__)

BUNDLE_DIR = Path("data/bundles")


def find_pitcher_id(pitcher_name: str) -> int:
    """Look up pitcher's MLB player ID by name."""
    if not pitcher_name:
        return None
    try:
        results = statsapi.lookup_player(pitcher_name)
        if results:
            return results[0]['id']
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(
        description='Step 2: Build game bundle from cached API data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/build_bundle.py CWS NYY 2025-09-25
        """
    )

    parser.add_argument('away_team', help='Away team abbreviation (e.g., CWS)')
    parser.add_argument('home_team', help='Home team abbreviation (e.g., NYY)')
    parser.add_argument('game_date', help='Game date (YYYY-MM-DD)')

    args = parser.parse_args()

    # Derive season from game date
    args.season = int(args.game_date.split('-')[0])

    print()
    print("=" * 60)
    print("STEP 2: BUILD GAME BUNDLE")
    print("=" * 60)
    print()
    print(f"Game: {args.away_team} @ {args.home_team}")
    print(f"Date: {args.game_date}")
    print()

    # Ensure bundle directory exists
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

    # Find pitcher info from schedule
    print("Looking up game info...")
    client = MLBStatsAPIClient()
    schedule = client.get_schedule(team=args.home_team, start_date=args.game_date, end_date=args.game_date)

    pitcher_names = None
    pitcher_ids = None

    if schedule:
        game = schedule[0]
        away_pitcher_name = game.get('away_probable_pitcher', '').strip()
        home_pitcher_name = game.get('home_probable_pitcher', '').strip()

        if away_pitcher_name or home_pitcher_name:
            pitcher_names = {}
            pitcher_ids = {}
            if away_pitcher_name:
                pitcher_names['away'] = away_pitcher_name
                pitcher_ids['away'] = find_pitcher_id(away_pitcher_name)
                print(f"  Away pitcher: {away_pitcher_name}")
            if home_pitcher_name:
                pitcher_names['home'] = home_pitcher_name
                pitcher_ids['home'] = find_pitcher_id(home_pitcher_name)
                print(f"  Home pitcher: {home_pitcher_name}")
    print()

    # Build the bundle using data_fetcher (which reads from API cache)
    print("Assembling game bundle from cached data...")
    print("(All data should come from API cache - no external calls)")
    print()

    data = fetch_game_data(
        away_team=args.away_team,
        home_team=args.home_team,
        game_date=args.game_date,
        season=args.season,
        pitcher_names=pitcher_names,
        pitcher_ids=pitcher_ids
    )

    if not data:
        print("✗ Failed to build bundle!")
        sys.exit(1)

    # Add bundle metadata
    data['bundle_metadata'] = {
        'away_team': args.away_team,
        'home_team': args.home_team,
        'game_date': args.game_date,
        'built_at': datetime.now().isoformat(),
        'season': args.season
    }

    # Save bundle
    bundle_filename = f"{args.away_team}_{args.home_team}_{args.game_date}.json"
    bundle_path = BUNDLE_DIR / bundle_filename

    with open(bundle_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    print()
    print("=" * 60)
    print("BUNDLE COMPLETE!")
    print("=" * 60)
    print()

    # Summary
    print(f"✓ Game: {data['away_team_full']} @ {data['home_team_full']}")
    print(f"  Venue: {data.get('venue', 'TBD')}")
    print(f"  Time: {data.get('game_time', 'TBD')}")
    print(f"  Records: {data.get('away_record', 'N/A')} vs {data.get('home_record', 'N/A')}")
    print()

    if 'away_pitcher' in data:
        p = data['away_pitcher']
        print(f"✓ {args.away_team} pitcher: {p['name']} ({p['wins']}-{p['losses']}, {p['ERA']:.2f} ERA)")
    if 'home_pitcher' in data:
        p = data['home_pitcher']
        print(f"✓ {args.home_team} pitcher: {p['name']} ({p['wins']}-{p['losses']}, {p['ERA']:.2f} ERA)")

    if 'away_lineup' in data:
        print(f"✓ {args.away_team} lineup: {len(data['away_lineup'])} batters")
    if 'home_lineup' in data:
        print(f"✓ {args.home_team} lineup: {len(data['home_lineup'])} batters")

    if 'schedule_context' in data:
        cal = data['schedule_context'].get('calendar', [])
        print(f"✓ Schedule: {len(cal)} days")

    if 'division_race_data' in data:
        for div in data['division_race_data']:
            print(f"✓ {div} standings")

    if 'home_re24_data' in data:
        print(f"✓ {args.home_team} RE24: {len(data['home_re24_data'])} players")
    if 'away_re24_data' in data:
        print(f"✓ {args.away_team} RE24: {len(data['away_re24_data'])} players")

    print()
    print(f"Bundle saved: {bundle_path}")
    print(f"Size: {bundle_path.stat().st_size / 1024:.1f} KB")
    print()

    print("Next step: Render preview")
    print(f"  python scripts/render_preview.py {args.away_team} {args.home_team} {args.game_date}")
    print()


if __name__ == '__main__':
    main()
