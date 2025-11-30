"""
PDF generator using Playwright headless browser.

Renders HTML templates to PDF using Chromium, ensuring identical
rendering to the interactive HTML dashboards.
"""

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright
import plotly.graph_objects as go

from visualization.chart_renderer import ChartRenderer
from config.logging_config import get_logger

logger = get_logger(__name__)


class PDFGenerator:
    """
    Generate PDF reports from Jinja2 templates using Playwright.

    Advantages over WeasyPrint:
    - Full modern CSS support (flexbox, grid, etc.)
    - JavaScript execution (Plotly charts render natively)
    - Identical rendering to HTML output
    - Better font support
    - More reliable complex layouts
    """

    def __init__(
        self,
        template_dir: Optional[str] = None,
        output_dir: Optional[str] = None
    ):
        """
        Initialize PDF generator.

        Args:
            template_dir: Directory containing Jinja2 templates
            output_dir: Directory for PDF output
        """
        # Set up template directory
        if template_dir is None:
            template_dir = Path(__file__).parent.parent / 'templates'
        self.template_dir = Path(template_dir)

        # Set up output directory
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / 'output' / 'pdfs'
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True
        )

        # Initialize chart renderer (HTML mode)
        self.chart_renderer = ChartRenderer()

        logger.info(f"PDFGenerator initialized with templates from {self.template_dir}")

    def generate_game_preview_pdf(
        self,
        data: Dict[str, Any],
        charts: Optional[Dict[str, go.Figure]] = None,
        output_filename: Optional[str] = None
    ) -> Path:
        """
        Generate game preview PDF.

        Args:
            data: Template context data (teams, pitchers, lineups, etc.)
            charts: Dict mapping chart_id -> Plotly Figure
            output_filename: Output filename (auto-generated if None)

        Returns:
            Path to generated PDF

        Example:
            >>> data = {
            ...     'away_team': 'NYY',
            ...     'home_team': 'BOS',
            ...     'game_date': '2024-06-15',
            ...     'away_pitcher': {...},
            ...     'home_pitcher': {...}
            ... }
            >>> charts = {
            ...     'pitcher_comparison': fig1,
            ...     'war_spider': fig2
            ... }
            >>> pdf_path = generator.generate_game_preview_pdf(data, charts)
        """
        # Add generation metadata
        data['output_format'] = 'pdf'
        data['generation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Render charts to HTML (they'll render natively in browser)
        if charts:
            rendered_charts = self.chart_renderer.render_multiple(charts)
            data.update(rendered_charts)

        # Render template to HTML
        html_content = self._render_template('game_preview.html', data)

        # Generate output filename if not provided
        if output_filename is None:
            away = data.get('away_team', 'AWAY')
            home = data.get('home_team', 'HOME')
            date = data.get('game_date', datetime.now().strftime('%Y%m%d'))
            output_filename = f'game_preview_{away}_at_{home}_{date}.pdf'

        # Generate PDF using Playwright
        output_path = self.output_dir / output_filename
        self._html_to_pdf(html_content, output_path)

        logger.info(f"Generated game preview PDF: {output_path}")
        return output_path

    def generate_custom_pdf(
        self,
        template_name: str,
        data: Dict[str, Any],
        charts: Optional[Dict[str, go.Figure]] = None,
        output_filename: str = 'output.pdf'
    ) -> Path:
        """
        Generate PDF from any custom template.

        Args:
            template_name: Name of template file (e.g., 'my_report.html')
            data: Template context data
            charts: Optional charts to embed
            output_filename: Output filename

        Returns:
            Path to generated PDF

        Example:
            >>> data = {'title': 'Season Report', 'stats': [...]}
            >>> pdf_path = generator.generate_custom_pdf('season_report.html', data)
        """
        # Add generation metadata
        data['output_format'] = 'pdf'
        data['generation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Render charts if provided
        if charts:
            rendered_charts = self.chart_renderer.render_multiple(charts)
            data.update(rendered_charts)

        # Render template
        html_content = self._render_template(template_name, data)

        # Generate PDF
        output_path = self.output_dir / output_filename
        self._html_to_pdf(html_content, output_path)

        logger.info(f"Generated custom PDF: {output_path}")
        return output_path

    def _render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """
        Render Jinja2 template to HTML string.

        Args:
            template_name: Name of template file
            data: Template context data

        Returns:
            Rendered HTML string
        """
        try:
            template = self.jinja_env.get_template(template_name)
            html = template.render(**data)
            logger.debug(f"Rendered template: {template_name}")
            return html

        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {e}", exc_info=True)
            raise

    def _html_to_pdf(
        self,
        html_content: str,
        output_path: Path,
        wait_for_charts: int = 2000
    ) -> None:
        """
        Convert HTML to PDF using Playwright's headless Chromium.

        Args:
            html_content: HTML string
            output_path: Path for PDF output
            wait_for_charts: Milliseconds to wait for Plotly charts to render
        """
        try:
            with sync_playwright() as p:
                # Launch headless browser (with args to prevent macOS dock icon)
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--hide-scrollbars',
                        '--mute-audio',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding'
                    ]
                )
                page = browser.new_page()

                # Set content and wait for charts to render
                page.set_content(html_content, wait_until='networkidle')

                # Wait for Plotly charts to fully render
                # Plotly uses JavaScript to draw charts, so we need to wait
                page.wait_for_timeout(wait_for_charts)

                # Generate PDF
                page.pdf(
                    path=str(output_path),
                    format='Letter',
                    print_background=True,
                    margin={
                        'top': '0.75in',
                        'right': '0.75in',
                        'bottom': '0.75in',
                        'left': '0.75in'
                    }
                )

                browser.close()

            logger.debug(f"Converted HTML to PDF using Playwright: {output_path}")

        except Exception as e:
            logger.error(f"Failed to generate PDF: {e}", exc_info=True)
            raise

    def save_html_preview(
        self,
        template_name: str,
        data: Dict[str, Any],
        charts: Optional[Dict[str, go.Figure]] = None,
        output_filename: str = 'preview.html'
    ) -> Path:
        """
        Save HTML preview of template (useful for debugging).

        Args:
            template_name: Template to render
            data: Template context
            charts: Optional charts
            output_filename: Output HTML filename

        Returns:
            Path to HTML file

        Use this to debug templates before generating PDF.
        """
        # Render for PDF format but save as HTML
        data['output_format'] = 'pdf'
        data['generation_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        if charts:
            rendered_charts = self.chart_renderer.render_multiple(charts)
            data.update(rendered_charts)

        html_content = self._render_template(template_name, data)

        # Save to file
        output_path = self.output_dir / output_filename
        output_path.write_text(html_content, encoding='utf-8')

        logger.info(f"Saved HTML preview: {output_path}")
        return output_path

    def get_available_templates(self) -> List[str]:
        """
        Get list of available template files.

        Returns:
            List of template filenames
        """
        templates = []
        for path in self.template_dir.rglob('*.html'):
            rel_path = path.relative_to(self.template_dir)
            templates.append(str(rel_path))

        return sorted(templates)


def create_pdf_from_game_data(
    game_data: Dict[str, Any],
    charts: Optional[Dict[str, go.Figure]] = None,
    output_filename: Optional[str] = None
) -> Path:
    """
    Convenience function to generate game preview PDF.

    Args:
        game_data: Game preview data
        charts: Optional Plotly charts
        output_filename: Output filename

    Returns:
        Path to generated PDF

    Example:
        >>> from output.pdf_generator import create_pdf_from_game_data
        >>> pdf_path = create_pdf_from_game_data(game_data, charts)
        >>> print(f"PDF saved to: {pdf_path}")
    """
    generator = PDFGenerator()
    return generator.generate_game_preview_pdf(game_data, charts, output_filename)
