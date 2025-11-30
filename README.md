# Baseball Stats - Game Preview Generator

Generates professional game preview PDFs with real MLB data from FanGraphs, Statcast, and MLB Stats API.

## Features

- **Real MLB data**: Pitcher stats, pitch mix, lineup stats, division race charts
- **Fast iteration**: Caching architecture separates data fetching from report generation
- **Print-optimized**: Clean, monochrome-friendly design inspired by Baseball Savant
- **Team logos & headshots**: Professional presentation with MLB assets

## Quick Start

### 1. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Fetch Game Data (once per game, ~60s)
```bash
python scripts/fetch_game_data.py NYY BOS 2025-09-25 \
    --away-pitcher "Marcus Stroman" \
    --home-pitcher "Brayan Bello" \
    --away-pitcher-id 573186 \
    --home-pitcher-id 676656
```

### 3. Generate Preview (fast, <5s)
```bash
python scripts/generate_preview.py NYY BOS 2025-09-25
```

Output: `output/pdfs/NYY_BOS_2025-09-25.pdf`

## Workflows

See [`scripts/README.md`](scripts/README.md) for complete workflow documentation.

**Workflow A (Fetch)**: Slow (~60s), run once per game
- Fetches from MLB API, FanGraphs, Statcast
- Caches to `data/cache/*.json`

**Workflow B (Generate)**: Fast (<5s), iterate on design
- Reads from cache (no API calls)
- Generates HTML and PDF

## Project Structure

```
baseball-stats/
├── scripts/              # Entry point workflows
│   ├── fetch_game_data.py
│   ├── generate_preview.py
│   └── README.md
├── ingestion/            # Data fetching
│   ├── data_fetcher.py      # Orchestrates all fetching
│   ├── mlb_api_client.py    # MLB Stats API
│   └── pybaseball_client.py # FanGraphs/Statcast
├── utils/                # Utilities
│   ├── data_cache.py        # JSON cache manager
│   ├── team_data.py         # Team logos, names
│   └── real_season_data.py  # Division race data
├── templates/            # Jinja2 templates
│   ├── base.html
│   └── game_preview.html
├── visualization/        # Chart generation
│   └── charts/
│       └── standings_chart.py
├── output/              # Generated files (gitignored)
│   ├── html/
│   └── pdfs/
└── data/cache/          # Cached data (gitignored)
```

## Data Sources

- **Game info**: MLB Stats API
- **Pitcher stats**: FanGraphs (via pybaseball)
- **Pitch mix**: Baseball Savant/Statcast (via pybaseball)
- **Division race**: MLB Stats API game logs

## Development

**Fast iteration** (12x faster):
1. Fetch data once: `python scripts/fetch_game_data.py ...`
2. Edit templates in `templates/`
3. Regenerate instantly: `python scripts/generate_preview.py ...`
4. Repeat steps 2-3

## Requirements

- Python 3.9+
- See `requirements.txt` for dependencies

## License

Private project
