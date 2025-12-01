#!/usr/bin/env python3
"""
Step 1: Fetch API data (with caching).

Fetches raw data from MLB API, Statcast, etc. and caches responses
at the endpoint level with 6-hour TTL.

This is the SLOW step that hits external APIs. Cached responses
are reused automatically.

Usage:
    python scripts/fetch_api_data.py NYY 2025-09-25
    python scripts/fetch_api_data.py NYY 2025-09-25 --force  # Clear cache first
"""

import argparse
import sys
from pathlib import Path
import statsapi

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.mlb_api_client import MLBStatsAPIClient
from ingestion.pybaseball_client import get_pitch_mix, get_batter_fangraphs_stats
from ingestion.weather_client import WeatherClient
from utils.api_cache import get_api_cache
from utils.real_season_data import fetch_division_teams_data
from utils.re24_calculator import get_batter_season_re24
from config.logging_config import get_logger

logger = get_logger(__name__)

# Team ID mappings
TEAM_ID_TO_ABBR = {
    147: 'NYY', 111: 'BOS', 139: 'TB', 141: 'TOR', 110: 'BAL',
    114: 'CLE', 142: 'MIN', 145: 'CWS', 116: 'DET', 118: 'KC',
    117: 'HOU', 140: 'TEX', 136: 'SEA', 108: 'LAA', 133: 'OAK',
    144: 'ATL', 143: 'PHI', 121: 'NYM', 146: 'MIA', 120: 'WSH',
    158: 'MIL', 138: 'STL', 112: 'CHC', 113: 'CIN', 134: 'PIT',
    119: 'LAD', 135: 'SD', 137: 'SF', 115: 'COL', 109: 'ARI'
}

DIVISION_TEAMS = {
    'AL East': ['NYY', 'BOS', 'TB', 'TOR', 'BAL'],
    'AL Central': ['CLE', 'MIN', 'CWS', 'DET', 'KC'],
    'AL West': ['HOU', 'TEX', 'SEA', 'LAA', 'OAK'],
    'NL East': ['ATL', 'PHI', 'NYM', 'MIA', 'WSH'],
    'NL Central': ['MIL', 'STL', 'CHC', 'CIN', 'PIT'],
    'NL West': ['LAD', 'SD', 'SF', 'COL', 'ARI'],
}

TEAM_TO_DIVISION = {}
for div, teams in DIVISION_TEAMS.items():
    for team in teams:
        TEAM_TO_DIVISION[team] = div


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
        description='Step 1: Fetch API data (cached at endpoint level)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/fetch_api_data.py NYY 2025-09-25
  python scripts/fetch_api_data.py NYY 2025-09-25 --force  # Clear cache
        """
    )

    parser.add_argument('team', help='Team abbreviation (e.g., NYY)')
    parser.add_argument('game_date', help='Game date (YYYY-MM-DD)')
    parser.add_argument('--force', action='store_true', help='Clear API cache first')

    args = parser.parse_args()

    # Derive season from game date
    args.season = int(args.game_date.split('-')[0])

    print()
    print("=" * 60)
    print("STEP 1: FETCH API DATA")
    print("=" * 60)
    print()

    # Handle force refresh
    cache = get_api_cache()
    if args.force:
        cleared = cache.clear_all()
        print(f"✓ Cleared {cleared} cached API responses")
        print()

    # Find the game
    print(f"Finding game for {args.team} on {args.game_date}...")
    client = MLBStatsAPIClient()
    schedule = client.get_schedule(team=args.team, start_date=args.game_date, end_date=args.game_date)

    if not schedule:
        print(f"✗ No game found for {args.team} on {args.game_date}")
        sys.exit(1)

    game = schedule[0]
    away_team_abbr = TEAM_ID_TO_ABBR.get(game.get('away_id'), 'UNK')
    home_team_abbr = TEAM_ID_TO_ABBR.get(game.get('home_id'), 'UNK')

    print(f"✓ Found: {away_team_abbr} @ {home_team_abbr}")
    print(f"  Venue: {game.get('venue_name', 'TBD')}")
    print()

    # Get pitcher info
    away_pitcher_name = game.get('away_probable_pitcher', '').strip()
    home_pitcher_name = game.get('home_probable_pitcher', '').strip()

    pitcher_ids = {}
    if away_pitcher_name:
        print(f"Away pitcher: {away_pitcher_name}")
        pitcher_ids['away'] = find_pitcher_id(away_pitcher_name)
        if pitcher_ids['away']:
            print(f"  ID: {pitcher_ids['away']}")
    if home_pitcher_name:
        print(f"Home pitcher: {home_pitcher_name}")
        pitcher_ids['home'] = find_pitcher_id(home_pitcher_name)
        if pitcher_ids['home']:
            print(f"  ID: {pitcher_ids['home']}")
    print()

    # Fetch all data (will use cache if available)
    print("Fetching data from APIs (cached responses reused)...")
    print()

    # 1. Team records
    print("  Team records...")
    client.get_team_record(away_team_abbr, args.season)
    client.get_team_record(home_team_abbr, args.season)

    # 2. Schedule context
    print("  Schedule context...")
    client.get_schedule_context(home_team_abbr, args.game_date)

    # 3. Pitcher stats
    if pitcher_ids.get('away'):
        print(f"  Away pitcher stats...")
        client.get_pitcher_season_stats(pitcher_ids['away'], args.season)
    if pitcher_ids.get('home'):
        print(f"  Home pitcher stats...")
        client.get_pitcher_season_stats(pitcher_ids['home'], args.season)

    # 4. Pitch mix (Statcast - expensive)
    if away_pitcher_name:
        print(f"  Away pitcher pitch mix (Statcast)...")
        get_pitch_mix(away_pitcher_name, args.season)
    if home_pitcher_name:
        print(f"  Home pitcher pitch mix (Statcast)...")
        get_pitch_mix(home_pitcher_name, args.season)

    # 5. Lineups
    game_id = game.get('game_id') or game.get('game_pk')
    if game_id:
        print("  Game lineups...")
        client.get_game_lineups(game_id)

        print("  Bench players...")
        client.get_game_bench_players(game_id)

    # 6. Batter stats for lineups
    lineups = client.get_game_lineups(game_id) if game_id else None
    if lineups:
        print("  Batter stats (away lineup)...")
        for batter in lineups.get('away', []):
            client.get_batter_season_stats(batter['player_id'], args.season)

        print("  Batter stats (home lineup)...")
        for batter in lineups.get('home', []):
            client.get_batter_season_stats(batter['player_id'], args.season)

    # 7. Division race data
    away_div = TEAM_TO_DIVISION.get(away_team_abbr)
    home_div = TEAM_TO_DIVISION.get(home_team_abbr)

    if away_div:
        print(f"  {away_div} standings...")
        fetch_division_teams_data(DIVISION_TEAMS[away_div], args.season)

    if home_div and home_div != away_div:
        print(f"  {home_div} standings...")
        fetch_division_teams_data(DIVISION_TEAMS[home_div], args.season)

    # 8. Weather data
    venue = game.get('venue_name')
    if venue:
        print(f"  Weather for {venue}...")
        try:
            weather_client = WeatherClient()
            weather_client.get_forecast_temperature(venue)
        except Exception as e:
            print(f"    (Weather fetch failed: {e})")

    # 9. FanGraphs stats for lineup batters
    if lineups:
        print("  FanGraphs stats (away lineup)...")
        for batter in lineups.get('away', []):
            get_batter_fangraphs_stats(batter['name'], args.season)

        print("  FanGraphs stats (home lineup)...")
        for batter in lineups.get('home', []):
            get_batter_fangraphs_stats(batter['name'], args.season)

    # 10. RE24 data for lineup batters
    if lineups:
        print("  RE24 data (away lineup)...")
        for batter in lineups.get('away', []):
            get_batter_season_re24(batter['player_id'], args.season, end_date=args.game_date)

        print("  RE24 data (home lineup)...")
        for batter in lineups.get('home', []):
            get_batter_season_re24(batter['player_id'], args.season, end_date=args.game_date)

    # 11. Head-to-head record
    print(f"  Head-to-head record ({home_team_abbr} vs {away_team_abbr})...")
    client.get_head_to_head_record(home_team_abbr, away_team_abbr, args.season, before_date=args.game_date)

    # 12. Bench player stats
    if game_id:
        bench_players = client.get_game_bench_players(game_id)
        if bench_players:
            print("  Bench player stats (away)...")
            for player in bench_players.get('away', []):
                client.get_batter_season_stats(player['player_id'], args.season)

            print("  Bench player stats (home)...")
            for player in bench_players.get('home', []):
                client.get_batter_season_stats(player['player_id'], args.season)

    # 13. Additional context
    print("  Injuries, transactions, leaders...")
    client.get_team_injuries(away_team_abbr)
    client.get_team_injuries(home_team_abbr)
    client.get_recent_transactions(args.game_date)
    client.get_league_leaders(args.season)

    print()
    print("=" * 60)
    print("FETCH COMPLETE!")
    print("=" * 60)
    print()

    # Show cache stats
    stats = cache.stats()
    print(f"API cache: {stats['valid_files']} responses ({stats['total_size_kb']:.1f} KB)")
    print()

    print("Next step: Build game bundle")
    print(f"  python scripts/build_bundle.py {away_team_abbr} {home_team_abbr} {args.game_date}")
    print()


if __name__ == '__main__':
    main()
