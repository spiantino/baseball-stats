# Baseball Stats Project Structure

## What to Commit

### Core Application Code
```
business_logic/         # Business logic layer (if used)
config/                 # Configuration files
  ├── logging_config.py
  └── settings.py
data_access/           # Data access layer (if used)
ingestion/             # Data fetching clients
  ├── mlb_api_client.py
  ├── pybaseball_client.py
  └── data_fetcher.py
output/                # Output generators
  ├── html_generator.py
  └── pdf_generator.py
services/              # Service layer (if used)
templates/             # Jinja2 templates
  ├── base.html
  ├── game_preview.html
  └── components/
utils/                 # Utility modules
  ├── data_cache.py
  ├── team_data.py
  └── real_season_data.py
visualization/         # Chart generation
  └── charts/
      └── standings_chart.py
```

### Scripts & Entry Points
```
scripts/               # Workflow entry points
  ├── README.md           # Workflow documentation
  ├── fetch_game_data.py  # Workflow A
  └── generate_preview.py # Workflow B
cli.py                 # CLI interface (if used)
```

### Configuration
```
.env.example           # Example environment variables
.gitignore             # Git ignore rules
requirements.txt       # Python dependencies
```

### Tests
```
tests/
  └── unit/            # Unit tests (keep these)
```

## What NOT to Commit (in .gitignore)

### Generated/Runtime Files
```
venv/                  # Virtual environment
data/cache/            # Cached game data (JSON files)
output/html/           # Generated HTML files
output/pdfs/           # Generated PDF files
logs/                  # Log files
```

### Development Artifacts
```
PHASE*.md              # Development phase docs
*_PROGRESS.md          # Progress tracking docs
*_COMPLETE.md          # Completion docs
JSON_STORAGE.md        # Dev notes
tests/archive/         # Archived test files
```

### Python/IDE
```
__pycache__/
*.pyc
.pytest_cache/
.vscode/
.idea/
```

## Directory Purpose

| Directory | Purpose | Commit? |
|-----------|---------|---------|
| `business_logic/` | Business rules and logic | ✓ |
| `config/` | App configuration | ✓ |
| `data/cache/` | Cached JSON data | ✗ (runtime) |
| `data_access/` | Database/storage layer | ✓ |
| `examples/` | Example usage | ✓ (if useful) |
| `ingestion/` | API clients & fetchers | ✓ |
| `output/` | Generators (code only) | ✓ |
| `output/html/` | Generated files | ✗ (runtime) |
| `output/pdfs/` | Generated files | ✗ (runtime) |
| `scripts/` | Entry point scripts | ✓ |
| `services/` | Service layer | ✓ |
| `templates/` | HTML templates | ✓ |
| `tests/unit/` | Unit tests | ✓ |
| `tests/archive/` | Old test files | ✗ |
| `utils/` | Utility modules | ✓ |
| `visualization/` | Chart generation | ✓ |
| `venv/` | Virtual environment | ✗ |

## Next Steps Before Committing

1. Review untracked files:
   ```bash
   git status
   ```

2. Check if all needed directories exist:
   ```bash
   ls -d */
   ```

3. Add all source code:
   ```bash
   git add business_logic config data_access ingestion output scripts services templates tests utils visualization
   git add .env.example .gitignore requirements.txt cli.py
   ```

4. Review what's staged:
   ```bash
   git status
   ```

5. Commit:
   ```bash
   git commit -m "Add caching architecture and workflow scripts"
   ```
