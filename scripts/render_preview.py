#!/usr/bin/env python3
"""
Step 3: Render preview from game bundle.

Reads the game bundle JSON and generates HTML/PDF output.
This step does NOT hit any APIs - it only reads the bundle file.

This is the FAST step for iterating on templates and styling.

Usage:
    python scripts/render_preview.py CWS NYY 2025-09-25
    python scripts/render_preview.py CWS NYY 2025-09-25 --html-only
    python scripts/render_preview.py CWS NYY 2025-09-25 --pdf-only
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd
import io

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.data_validator import validate_game_data, print_data_summary, DataValidationError
from visualization.charts.standings_chart import create_division_race_chart, create_re24_chart
from output.html_generator import HTMLGenerator
from output.pdf_generator import PDFGenerator
from config.logging_config import get_logger

logger = get_logger(__name__)

BUNDLE_DIR = Path("data/bundles")


def compute_series_re24(data: dict) -> dict:
    """
    Compute RE24 totals for current series, previous series, and season.

    Returns dict with lineup_re24 containing per-player RE24 breakdown.
    """
    result = {'home_lineup_re24': [], 'away_lineup_re24': []}

    game_date = data.get('game_date')
    away_team = data.get('away_team')
    schedule = data.get('schedule_context', {}).get('calendar', [])

    if not schedule or not game_date:
        return result

    # Find current series dates (consecutive games vs same opponent including today)
    current_series_dates = []
    prev_series_dates = []
    prev_series_opponent = None

    # Sort calendar by date
    sorted_schedule = sorted(schedule, key=lambda x: x['game_date'])

    # Find the game date index
    game_idx = None
    for i, g in enumerate(sorted_schedule):
        if g['game_date'] == game_date:
            game_idx = i
            break

    if game_idx is None:
        return result

    # Find current series (games with same opponent around game_date)
    current_opponent = away_team  # The opponent in this game

    # Helper to normalize opponent abbreviation (remove "@ " or "vs " prefix)
    def normalize_opp(opp_str: str) -> str:
        return opp_str.replace('@ ', '').replace('vs ', '').strip()

    # Look backwards to find start of current series
    start_idx = game_idx
    for i in range(game_idx - 1, -1, -1):
        g = sorted_schedule[i]
        opp = normalize_opp(g.get('opponent_abbr', ''))
        if opp == current_opponent:
            start_idx = i
        else:
            break

    # Look forwards to find end of current series (for series that extend past today)
    end_idx = game_idx
    for i in range(game_idx + 1, len(sorted_schedule)):
        g = sorted_schedule[i]
        opp = normalize_opp(g.get('opponent_abbr', ''))
        if opp == current_opponent:
            end_idx = i
        else:
            break

    # Get current series dates (only up to and including today)
    for i in range(start_idx, game_idx + 1):
        current_series_dates.append(sorted_schedule[i]['game_date'])

    # Find previous series (look backwards from current series start)
    if start_idx > 0:
        prev_end_idx = start_idx - 1
        # Skip off days
        while prev_end_idx >= 0 and sorted_schedule[prev_end_idx].get('status') == 'OFF':
            prev_end_idx -= 1

        if prev_end_idx >= 0:
            prev_series_opponent = normalize_opp(sorted_schedule[prev_end_idx].get('opponent_abbr', ''))
            prev_start_idx = prev_end_idx

            # Find start of previous series
            for i in range(prev_end_idx - 1, -1, -1):
                g = sorted_schedule[i]
                opp = normalize_opp(g.get('opponent_abbr', ''))
                if opp == prev_series_opponent:
                    prev_start_idx = i
                else:
                    break

            for i in range(prev_start_idx, prev_end_idx + 1):
                prev_series_dates.append(sorted_schedule[i]['game_date'])

    # Helper to sum RE24 for last N games
    def sum_re24_last_n(player_games: list, n: int, before_date: str) -> float:
        # Filter games before the given date and take last N
        games_before = [g for g in player_games if g.get('game_date', '') < before_date]
        last_n = games_before[-n:] if len(games_before) >= n else games_before
        return sum(g.get('re24', 0) for g in last_n)

    # Process home team lineup
    if 'home_lineup' in data and 'home_re24_data' in data:
        for batter in data['home_lineup']:
            player_id = batter.get('player_id')
            name = batter.get('name', '')

            # Get last name (handle suffixes)
            name_parts = name.split() if name else []
            suffixes = {'Jr.', 'Sr.', 'II', 'III', 'IV', 'V'}
            if len(name_parts) >= 2:
                last_name = name_parts[-1]
                if last_name in suffixes and len(name_parts) >= 3:
                    last_name = name_parts[-2]
            else:
                last_name = name

            # Find this player's RE24 data
            player_games = data['home_re24_data'].get(last_name, [])

            # Calculate RE24 sums
            season_re24 = player_games[-1]['cumulative_re24'] if player_games else 0
            l10_re24 = sum_re24_last_n(player_games, 10, game_date)

            result['home_lineup_re24'].append({
                'name': name,
                'position': batter.get('position', ''),
                'age': batter.get('age', 0),
                'slash': batter.get('slash', '.000/.000/.000'),
                'hr': batter.get('hr', 0),
                'rbi': batter.get('rbi', 0),
                'tb': batter.get('tb', 0),
                'ops': batter.get('ops', 0),
                'ops_plus': batter.get('ops_plus', 100),
                'owar': batter.get('owar', 0),
                'dwar': batter.get('dwar', 0),
                're24_season': round(season_re24, 1),
                're24_l10': round(l10_re24, 1),
            })

    # Process away team lineup
    if 'away_lineup' in data and 'away_re24_data' in data:
        for batter in data['away_lineup']:
            player_id = batter.get('player_id')
            name = batter.get('name', '')

            # Get last name (handle suffixes)
            name_parts = name.split() if name else []
            suffixes = {'Jr.', 'Sr.', 'II', 'III', 'IV', 'V'}
            if len(name_parts) >= 2:
                last_name = name_parts[-1]
                if last_name in suffixes and len(name_parts) >= 3:
                    last_name = name_parts[-2]
            else:
                last_name = name

            # Find this player's RE24 data
            player_games = data['away_re24_data'].get(last_name, [])

            # Calculate RE24 sums
            season_re24 = player_games[-1]['cumulative_re24'] if player_games else 0
            l10_re24 = sum_re24_last_n(player_games, 10, game_date)

            result['away_lineup_re24'].append({
                'name': name,
                'position': batter.get('position', ''),
                'age': batter.get('age', 0),
                'slash': batter.get('slash', '.000/.000/.000'),
                'hr': batter.get('hr', 0),
                'rbi': batter.get('rbi', 0),
                'tb': batter.get('tb', 0),
                'ops': batter.get('ops', 0),
                'ops_plus': batter.get('ops_plus', 100),
                'owar': batter.get('owar', 0),
                'dwar': batter.get('dwar', 0),
                're24_season': round(season_re24, 1),
                're24_l10': round(l10_re24, 1),
            })

    # Add metadata about series
    result['current_series_dates'] = current_series_dates
    result['current_series_game_num'] = len(current_series_dates)
    result['prev_series_dates'] = prev_series_dates
    result['prev_series_opponent'] = prev_series_opponent

    # Determine series location prefix (vs or @) for previous series
    if prev_series_dates and sorted_schedule:
        # Find a game from previous series to check location
        for g in sorted_schedule:
            if g['game_date'] in prev_series_dates:
                opp_str = g.get('opponent_abbr', '')
                if opp_str.startswith('@ '):
                    result['prev_series_location'] = '@ '
                else:
                    result['prev_series_location'] = 'vs '
                break

    # Calculate series record (games already played in current series, not including today)
    # and season record (all games vs this opponent this season)
    home_team = data.get('home_team')
    series_wins = 0
    series_losses = 0
    season_wins = 0
    season_losses = 0

    for g in sorted_schedule:
        opp = normalize_opp(g.get('opponent_abbr', ''))
        if opp != current_opponent:
            continue

        game_result = g.get('result', '')
        if not game_result:
            continue

        # Check if this is in current series (before today)
        if g['game_date'] in current_series_dates and g['game_date'] < game_date:
            if game_result == 'W':
                series_wins += 1
            elif game_result == 'L':
                series_losses += 1

        # Count all games vs this opponent for season record (before today only)
        if g['game_date'] < game_date:
            if game_result == 'W':
                season_wins += 1
            elif game_result == 'L':
                season_losses += 1

    # Find total games in current series (including future games)
    total_series_games = end_idx - start_idx + 1

    result['series_wins'] = series_wins
    result['series_losses'] = series_losses
    result['series_total_games'] = total_series_games
    result['season_wins'] = season_wins
    result['season_losses'] = season_losses

    return result


def fig_to_svg(fig) -> str:
    """Convert Matplotlib figure to SVG string."""
    output = io.StringIO()
    fig.savefig(output, format='svg', bbox_inches='tight')
    svg_string = output.getvalue()
    output.close()
    return svg_string


def main():
    parser = argparse.ArgumentParser(
        description='Step 3: Render preview from game bundle (fast)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/render_preview.py CWS NYY 2025-09-25
  python scripts/render_preview.py CWS NYY 2025-09-25 --html-only
  python scripts/render_preview.py CWS NYY 2025-09-25 --pdf-only
        """
    )

    parser.add_argument('away_team', help='Away team abbreviation (e.g., CWS)')
    parser.add_argument('home_team', help='Home team abbreviation (e.g., NYY)')
    parser.add_argument('game_date', help='Game date (YYYY-MM-DD)')
    parser.add_argument('--output', help='Output filename (without extension)')
    parser.add_argument('--html-only', action='store_true', help='Generate HTML only')
    parser.add_argument('--pdf-only', action='store_true', help='Generate PDF only')

    args = parser.parse_args()

    print()
    print("=" * 60)
    print("STEP 3: RENDER PREVIEW")
    print("=" * 60)
    print()
    print(f"Game: {args.away_team} @ {args.home_team}")
    print(f"Date: {args.game_date}")
    print()

    # Load bundle
    bundle_filename = f"{args.away_team}_{args.home_team}_{args.game_date}.json"
    bundle_path = BUNDLE_DIR / bundle_filename

    if not bundle_path.exists():
        print(f"✗ Bundle not found: {bundle_path}")
        print()
        print("Run build step first:")
        print(f"  python scripts/build_bundle.py {args.away_team} {args.home_team} {args.game_date}")
        print()
        sys.exit(1)

    print(f"Loading bundle: {bundle_path.name}")
    with open(bundle_path, 'r') as f:
        data = json.load(f)

    built_at = data.get('bundle_metadata', {}).get('built_at', 'unknown')
    print(f"  Built at: {built_at}")
    print()

    # Validate data
    print("Validating data...")
    try:
        validation_result = validate_game_data(data, strict=True)
        print_data_summary(data)

        if validation_result['warnings']:
            print()
            print(f"⚠ {len(validation_result['warnings'])} warnings (proceeding anyway)")

        print()
        print("✓ Data validation passed")
        print()
    except DataValidationError as e:
        print()
        print("✗ DATA VALIDATION FAILED")
        print()
        print(str(e))
        print()
        print("Rebuild bundle:")
        print(f"  python scripts/build_bundle.py {args.away_team} {args.home_team} {args.game_date}")
        print()
        sys.exit(1)

    # Generate division race charts
    charts = {}

    if 'division_race_data' in data:
        logger.info("Creating division race charts...")

        divisions_to_plot = []
        home_div = data.get('home_division')
        away_div = data.get('away_division')

        if home_div:
            divisions_to_plot.append(home_div)
        if away_div and away_div != home_div:
            divisions_to_plot.append(away_div)

        for division in divisions_to_plot:
            if division not in data['division_race_data']:
                continue

            team_data = data['division_race_data'][division]
            try:
                logger.info(f"  Creating chart for {division}...")

                # Convert cached data back to DataFrames
                team_dataframes = {}
                for team, records in team_data.items():
                    team_dataframes[team] = pd.DataFrame(records)

                # Determine which teams are playing
                playing_teams = []
                if data['away_division'] == division:
                    playing_teams.append(data['away_team'])
                if data['home_division'] == division:
                    playing_teams.append(data['home_team'])

                # Create chart
                chart_fig = create_division_race_chart(
                    team_dataframes,
                    division,
                    playing_teams=playing_teams,
                    figsize=(5.5, 4),
                    dpi=100,
                    show_y_labels=False
                )

                # Convert to SVG
                chart_svg = fig_to_svg(chart_fig)

                if not charts:
                    charts['division_race_chart'] = chart_svg
                elif 'division_race_chart' in charts:
                    charts['division_race_chart_2'] = chart_svg
                else:
                    charts['division_race_chart'] = chart_svg

                logger.info(f"   {division} chart created")

            except Exception as e:
                logger.warning(f"Could not create division chart for {division}: {e}")

    # Compute series RE24 data for lineup table
    series_re24_data = {}
    if 'home_re24_data' in data or 'away_re24_data' in data:
        logger.info("Computing series RE24 data...")
        try:
            series_re24_data = compute_series_re24(data)
            if series_re24_data.get('home_lineup_re24'):
                logger.info(f"   Computed RE24 for {len(series_re24_data['home_lineup_re24'])} {data['home_team']} players")
                logger.info(f"   Current series: game {series_re24_data.get('current_series_game_num', 0)} vs {data['away_team']}")
                if series_re24_data.get('prev_series_opponent'):
                    logger.info(f"   Previous series: vs {series_re24_data['prev_series_opponent']}")
        except Exception as e:
            logger.warning(f"Could not compute series RE24: {e}")

    # Determine output filename
    if args.output:
        output_filename = args.output
    else:
        output_filename = f"{args.away_team}_{args.home_team}_{args.game_date}"

    # Generate outputs
    html_gen = HTMLGenerator()
    pdf_gen = PDFGenerator()

    html_path = None
    pdf_path = None

    print("Generating preview files...")

    # Merge series RE24 data into main data dict for template access
    if series_re24_data:
        data.update(series_re24_data)

    try:
        if not args.pdf_only:
            html_path = html_gen.generate_game_preview_html(
                data=data,
                charts=charts,
                output_filename=f'{output_filename}.html'
            )
            print(f"   HTML: {html_path} ({html_path.stat().st_size / 1024:.1f} KB)")

        if not args.html_only:
            pdf_path = pdf_gen.generate_game_preview_pdf(
                data=data,
                charts=charts,
                output_filename=f'{output_filename}.pdf'
            )
            print(f"   PDF:  {pdf_path} ({pdf_path.stat().st_size / 1024:.1f} KB)")

        print()
        print("=" * 60)
        print("RENDER COMPLETE!")
        print("=" * 60)
        print()

        if html_path:
            print(f"HTML: {html_path}")
        if pdf_path:
            print(f"PDF:  {pdf_path}")
        print()

    except Exception as e:
        logger.error(f"Failed to render preview: {e}", exc_info=True)
        print()
        print(f"✗ Error: {e}")
        print()
        sys.exit(1)


if __name__ == '__main__':
    main()
