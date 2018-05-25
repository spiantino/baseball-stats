import pandas as pd
import jinja2
import tempfile
import subprocess
import os

class Latex:
    def __init__(self, filename):
        self._filename = filename
        self._f = open(filename, "w")

    def format_row_value(s, f=None):
        try:
            s = float(s)
        except:
            pass

        percent = False
        if isinstance(s, str) and s.endswith('%') and False:
            s = float(s.replace('%',''))
            percent = True

        if f is not None and f != '' and isinstance(s, float):
            formatted = f.format(s)
        else:
            formatted = Latex.tex_escape(s)

        if percent is True:
            return formatted + " \\%"
        else:
            return formatted
    
    def format_col_name(s):
        return Latex.tex_escape(s)

    def tex_escape(s):
        if isinstance(s, str):
            s = s.replace('%', '\\%')
            s = s.replace('#', '\\#')
            s = s.replace('_', ' ')
        return s

    def pd_to_rows(self, pd, formats=None):
        
        rows = pd.to_dict(orient='records')
        if formats is None:
            formats = [{}] * len(rows[0].keys())

        for i, row in enumerate(rows):
            rows[i] = [Latex.format_row_value(v,f) for v, f in zip(row.values(), formats)]
        
        t = jinja2.Template(
            r"""
            {% for row in data %}{% for v in row %} {{ v }} {% if not loop.last %}&{% endif %}{% endfor %} \\
            {% if not loop.last %}\hdashline{% endif %}{% endfor %}
            """)

        return t.render(data = rows)

    def header(self):
        self._f.write(r"""
                \documentclass{article}
                \usepackage[margin=0.4in,a4paper]{geometry}
                \usepackage{booktabs}
                \usepackage{caption}
                \usepackage{float}
                \usepackage{titlesec}
                \usepackage{capt-of}

                %dashed line
                \usepackage{array}
                \usepackage{arydshln}
                \setlength\dashlinedash{0.2pt}
                \setlength\dashlinegap{1.5pt}
                \setlength\arrayrulewidth{0.3pt}

                %Widows & Orphans & Penalties

                \widowpenalty500
                \clubpenalty500
                \clubpenalty=9996
                \exhyphenpenalty=50 %for line-breaking at an explicit hyphen
                \brokenpenalty=4991
                \predisplaypenalty=10000
                \postdisplaypenalty=1549
                \displaywidowpenalty=1602
                \floatingpenalty = 20000

                \usepackage[T1]{fontenc}
                \usepackage{fontspec}
                \setmainfont[Scale=0.85, Ligatures={Required,Common,Contextual,TeX}]{TeX Gyre Schola} % Incredible font inside latex


                \begin{document}
                """)

    def title(self, summary):
        t = jinja2.Template(
            r"""
            \section*{ Game {{ game }}: {{ title }} }
            \subsection*{ {{ details }} $\cdot$ {{ temp }}$\,^{\circ}$ {{ condition }} $\cdot$ Wind {{ wind }} }
            """)
        self._f.write( 
            t.render(game = summary['game'], 
                     title = summary['title'],
                     details = summary['details'],
                     temp = summary['temp'],
                     condition = summary['condition'],
                     wind = summary['wind'],
                     pitchers = summary['pit_df'].to_dict(orient='records')
                     )
        )

    def add_section(self, title):
        t = jinja2.Template(
            r"""
            \subsection*{ {{ title }} }
            """)
        self._f.write( 
            t.render(title = title)
        )

    def add_subsection(self, title):
        t = jinja2.Template(
            r"""
            \subsubsection*{ {{ title }} }
            """)
        self._f.write( 
            t.render(title = title)
        )

    def start_table(self, coldef):
        t = jinja2.Template(
           r"""
           \begin{tabular}{@{}{{ coldef }}@{}}
           """)
        self._f.write(t.render(coldef = coldef))

    def add_divider(self):
        t = jinja2.Template(
           r"""
           \midrule
           """)
        self._f.write(t.render())

    def end_table(self):
        t = jinja2.Template(
           r"""
           \bottomrule
           \end{tabular}
           """)
        self._f.write(t.render())


    def add_headers(self, columns):
        t = jinja2.Template(
            r"""
            {% for c in columns %}\textbf{ {{- c -}} }{% if not loop.last %} & {% endif %}{% endfor %} \\ \midrule
            """)

        self._f.write(
            t.render(columns = [Latex.format_col_name(c) for c in columns])
        )

    def add_rows(self, pd, formats=None):
        rows = pd.to_dict(orient='records')
        if formats is None:
            formats = [{}] * len(rows[0].keys())

        for i, row in enumerate(rows):
            rows[i] = [Latex.format_row_value(v,f) for v, f in zip(row.values(), formats)]
        
        t = jinja2.Template(
            r"""
            {% for row in data %}{% for v in row %} {{ v }} {% if not loop.last %}&{% endif %}{% endfor %} \\
            {% if not loop.last %}\hdashline{% endif %}{% endfor %}
            """)

        self._f.write(t.render(data = rows))


    def footer(self):
        self._f.write(r"""
            \end{document}
            """)

    def make_pdf(self):
        self._f.close()
        subprocess.call(['xelatex', self._filename])
        
        


