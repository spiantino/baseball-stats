#!/usr/bin/env python3
"""
Workflow B: Generate preview from cached data.

Generates HTML and PDF game previews using cached data. NO API CALLS.
This is the FAST operation for iterating on design and templates.

Usage:
    python scripts/generate_preview.py NYY BOS 2025-09-25
"""

import argparse
import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.data_cache import DataCache
from visualization.charts.standings_chart import create_division_race_chart
from output.html_generator import HTMLGenerator
from output.pdf_generator import PDFGenerator
from config.logging_config import get_logger

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Generate game preview from cached data (fast)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate preview from cache
  python scripts/generate_preview.py NYY BOS 2025-09-25

  # Generate only HTML
  python scripts/generate_preview.py NYY BOS 2025-09-25 --html-only

  # Generate only PDF
  python scripts/generate_preview.py NYY BOS 2025-09-25 --pdf-only

  # Custom output filename
  python scripts/generate_preview.py NYY BOS 2025-09-25 --output yankees_redsox
        """
    )

    parser.add_argument('away_team', help='Away team abbreviation (e.g., NYY)')
    parser.add_argument('home_team', help='Home team abbreviation (e.g., BOS)')
    parser.add_argument('game_date', help='Game date (YYYY-MM-DD)')

    # Output control
    parser.add_argument('--output', help='Output filename (without extension)')
    parser.add_argument('--html-only', action='store_true', help='Generate HTML only')
    parser.add_argument('--pdf-only', action='store_true', help='Generate PDF only')

    args = parser.parse_args()

    print()
    print("="*60)
    print("GENERATE PREVIEW (Workflow B)")
    print("="*60)
    print()
    print(f"Game: {args.away_team} @ {args.home_team}")
    print(f"Date: {args.game_date}")
    print()

    # Load cached data
    cache = DataCache()

    if not cache.has_cache(args.away_team, args.home_team, args.game_date):
        print("L No cached data found!")
        print()
        print("Run fetch workflow first:")
        print(f"  python scripts/fetch_game_data.py {args.away_team} {args.home_team} {args.game_date}")
        print()
        sys.exit(1)

    print("Loading cached data...")
    data = cache.get(args.away_team, args.home_team, args.game_date)

    if not data:
        print("L Failed to load cache!")
        sys.exit(1)

    print(f" Loaded cache from {data['cache_metadata']['fetched_at']}")
    print()

    # Generate division race charts
    charts = {}

    if 'division_race_data' in data:
        print("Generating division race charts...")

        for division, team_data in data['division_race_data'].items():
            # Convert cached data back to DataFrames
            team_dataframes = {}
            for team, records in team_data.items():
                team_dataframes[team] = pd.DataFrame(records)

            chart = create_division_race_chart(team_dataframes, division)

            # Use appropriate chart key
            if division == data.get('away_division'):
                charts['division_race_chart'] = chart
            elif division == data.get('home_division'):
                if data.get('away_division') == data.get('home_division'):
                    charts['division_race_chart'] = chart
                else:
                    charts['division_race_chart_2'] = chart

            print(f"   {division} chart created")

        print()

    # Determine output filename
    if args.output:
        output_filename = args.output
    else:
        output_filename = f"{args.away_team}_{args.home_team}_{args.game_date}"

    # Generate HTML and PDF
    html_gen = HTMLGenerator()
    pdf_gen = PDFGenerator()

    html_path = None
    pdf_path = None

    print("Generating preview files...")

    try:
        if not args.pdf_only:
            html_path = html_gen.generate_game_preview_html(
                data=data,
                charts=charts,
                output_filename=f'{output_filename}.html'
            )
            print(f"   HTML: {html_path} ({html_path.stat().st_size / 1024:.1f} KB)")

        if not args.html_only:
            pdf_path = pdf_gen.generate_game_preview_pdf(
                data=data,
                charts=charts,
                output_filename=f'{output_filename}.pdf'
            )
            print(f"   PDF:  {pdf_path} ({pdf_path.stat().st_size / 1024:.1f} KB)")

        print()
        print("="*60)
        print("GENERATION COMPLETE!")
        print("="*60)
        print()

        if html_path:
            print(f"HTML: {html_path}")
        if pdf_path:
            print(f"PDF:  {pdf_path}")
        print()

    except Exception as e:
        logger.error(f"Failed to generate preview: {e}", exc_info=True)
        print()
        print(f"L Error: {e}")
        print()
        sys.exit(1)


if __name__ == '__main__':
    main()
