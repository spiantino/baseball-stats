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
                \usepackage{multicol}
                \usepackage{graphicx}

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
                \setmainfont[Scale=0.80]{TeX Gyre Schola}

                \begin{document}
                """)

    def logos(self, home, away):
        self._f.write(r"""
            \graphicspath{{{{logos/}}}}
            \includegraphics[scale=0.5]{{{}.png}}
            \includegraphics[scale=0.5]{{{}.png}}""".format(away, home)
            )

    def title(self, summary, pitchers):
        t = jinja2.Template(
            r"""
            \section*{ Game {{ game }}: {{ title }} }
            \subsection*{ {{ details }} $\cdot$ {{ temp }}$\,^{\circ}$ {{ condition }} $\cdot$ Wind {{ wind }} }
            """)
        self._f.write(
            t.render(
                     game = summary['game'],
                     title = summary['title'],
                     details = summary['details'],
                     temp = summary['temp'],
                     condition = summary['condition'],
                     wind = summary['wind'],
                     pitchers = pitchers.to_dict(orient='records')
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

    def start_multicol(self, cols):
        t = jinja2.Template(
           r"""
            \begin{multicols}{ {{- cols -}} }
           """)
        self._f.write(t.render(cols = cols))

    def end_multicol(self):
        t = jinja2.Template(
           r"""
            \end{multicols}
           """)
        self._f.write(t.render())

    def page_break(self):
        t = jinja2.Template(
           r"""
            \newpage
           """)
        self._f.write(t.render())

    def vspace(self):
        t = jinja2.Template(
            r"""
            \vspace*{1mm}
            """)
        self._f.write(t.render())

    def add_headers(self, columns):
        t = jinja2.Template(
            r"""
            {% for c in columns %}\textbf{ {{- c -}} }{% if not loop.last %} & {% endif %}{% endfor %} \tabularnewline \midrule
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
            {% for row in data %}{% for v in row %} {{ v }}{% if not loop.last %}&{% endif %}{% endfor %} \tabularnewline
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
        

def make_pdf(team, date, home, away, summary, pitchers, starters, bench,
             bullpen, standings, history, bat_df, hr_df, rbi_df,
             pit_df, era_df, rel_df, elo_df, pit_hist, last_week_bp,
             series_table, gb, injuries, txs, upcoming):

    l = Latex("{}-{}.tex".format(team, date))
    l.header()
    l.logos(summary['home'], summary['away'])
    l.title(summary, pitchers)

    l.start_table('lcclrrrrrrrrrr')
    l.add_headers(['Team', 'R/L', '#', 'Name', 'war', 'w', 'l',
                   'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'whip', 'gb%'])
    l.add_rows(pitchers, ['', '', '{:.0f}', '', '{:.1f}',
                          '{:.0f}', '{:.0f}', '{:.2f}',
                          '{:.1f}', '{:.2f}', '{:.2f}',
                          '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("{} Lineup".format(away))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash',
                   'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[0], ['{:.0f}', '', '{:.0f}', '',
                             '{:.1f}', '', '{:.0f}', '{:.0f}',
                             '{:.0f}', '{:.1f}', '{:.1f}'])
    if not bench[0].empty:
        l.add_divider()
        l.add_rows(bench[0], ['{:.0f}', '', '{:.0f}', '',
                              '{:.1f}', '', '{:.0f}', '{:.0f}',
                              '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("{} Lineup".format(home))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash', 'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[1], ['{:.0f}', '', '{:.0f}', '',
                             '{:.1f}', '', '{:.0f}', '{:.0f}',
                             '{:.0f}', '{:.1f}', '{:.1f}'])
    if not bench[1].empty:
        l.add_divider()
        l.add_rows(bench[1], ['{:.0f}', '', '{:.0f}', '',
                              '{:.1f}', '', '{:.0f}', '{:.0f}',
                              '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("Standings")
    l.start_multicol(2)
    for table in standings:
        l.start_table('lrrcccrr')
        l.add_headers([table.columns[0], 'w', 'l', 'l10', 'gb',
                                         'strk', 'home', 'away'])
        l.add_rows(table, ['', '{:.0f}', '{:.0f}',
                           '', '{:.1f}', '', '{:.3f}', '{:.3f}'])
        l.end_table()
    l.end_multicol()

    l.page_break()
    l.add_subsection("{} Bullpen".format(home))
    l.start_table('lcrrrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip',
                   'k/9', 'bb/9', 'hr/9', 'whip', 'gb%', 'days'])
    l.add_rows(bullpen[0], ['', '{:.0f}', '{:.1f}', '{:.0f}',
                                '{:.2f}', '{:.1f}', '{:.1f}',
                                '{:.1f}', '{:.1f}', '{:.2f}',
                                '{:.1f}', '{:.0f}'])
    l.end_table()

    l.add_subsection("{} Bullpen".format(away))
    l.start_table('lcrrrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip',
                   'k/9', 'bb/9', 'hr/9', 'whip', 'gb%', 'days'])
    l.add_rows(bullpen[1], ['', '{:.0f}', '{:.1f}', '{:.0f}',
                                '{:.2f}', '{:.1f}', '{:.1f}',
                                '{:.1f}', '{:.1f}', '{:.2f}',
                                '{:.1f}', '{:.0f}'])
    l.end_table()

    l.add_section("{} Recent Starts".format(team))
    l.start_table('lclrrrrrrrr')
    l.add_headers(['Date', 'Opp', 'Starter', 'ip',
                   'h', 'r', 'er', 'bb', 'k', 'gs'])
    l.add_rows(pit_hist, ['', '', '', '{:.1f}', '{:.0f}',
                                      '{:.0f}', '{:.0f}',
                                      '{:.0f}', '{:.0f}',
                                      '{:.0f}', '{:.0f}'])
    l.end_table()
    # l.page_break()

    l.add_subsection("{} Injuries".format(away))
    l.start_table('llcp{10cm}')
    l.add_headers(['Last Updated', 'Name', 'Type', 'Details'])
    l.add_rows(injuries[0], ['', '', '', ''])
    l.end_table()

    l.add_subsection("{} Injuries".format(home))
    l.start_table('llcp{10cm}')
    l.add_headers(['Last Updated', 'Name', 'Type', 'Details'])
    l.add_rows(injuries[1], ['', '', '', ''])
    l.end_table()

    l.add_subsection("{} Recent Transactions".format(away))
    l.start_table('llcp{10cm}')
    l.add_headers(['Transaction Date', 'Player', 'Type', 'Note'])
    l.add_rows(txs[0], ['', '', '', ''])
    l.end_table()

    l.add_subsection("{} Recent Transactions".format(home))
    l.start_table('llcp{10cm}')
    l.add_headers(['Transaction Date', 'Player', 'Type', 'Note'])
    l.add_rows(txs[1], ['', '', '', ''])
    l.end_table()
    l.page_break()

    # l.add_subsection("{} Game Log".format(away))
    # l.start_table('lrlccc')
    # l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    # l.add_rows(history[1], ['', '', '', '', '', ''])
    # l.end_table()
    # l.page_break()

    # l.add_subsection("{} Game Log".format(home))
    # l.start_table('lrlccc')
    # l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    # l.add_rows(history[0], ['', '', '', '', '', ''])
    # l.end_table()
    # l.page_break()

    l.add_subsection("{} Game Log".format(away))
    l.start_multicol(2)
    l.start_table('lrlccc')
    l.add_headers(['Gm#', 'Date', 'Field', 'Opp', 'Time'])
    l.add_rows(upcoming[0], ['{:.0f}', '', '', '', ''])
    l.end_table()
    for table in history[1]:
        l.start_table('lrlccc')
        l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
        l.add_rows(table, ['', '', '', '', '', ''])
        l.end_table()
    l.end_multicol()
    l.page_break()

    l.add_subsection("{} Game Log".format(home))
    l.start_multicol(2)
    l.start_table('lrlccc')
    l.add_headers(['Gm#', 'Date', 'Field', 'Opp', 'Time'])
    l.add_rows(upcoming[1], ['{:.0f}', '', '', '', ''])
    l.end_table()
    for table in history[0]:
        l.start_table('lrlccc')
        l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
        l.add_rows(table, ['', '', '', '', '', ''])
        l.end_table()
    l.end_multicol()
    l.page_break()

    l.add_section("Batting Leaderboards")
    l.start_table('rllrcrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'slash', 'hr', 'rbi',
                       'sb', 'bb%', 'k%', 'babip', 'owar', 'dwar'])
    l.add_rows(bat_df, ['{:.0f}', '', '', '{:.1f}', '',
                        '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}',
                        '{:.1f}', '{:.3f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.start_multicol(2)
    l.add_subsection("HR")
    l.start_table('llrr')
    l.add_headers(['#', 'Name','Team', 'hr'])
    l.add_rows(hr_df, ['{:.0f}', '', '', '{:.0f}'])
    l.end_table()

    l.add_subsection("RBI")
    l.start_table('llrr')
    l.add_headers(['#', 'Name','Team', 'rbi'])
    l.add_rows(rbi_df, ['{:.0f}', '', '', '{:.0f}'])
    l.end_table()
    l.end_multicol()

    l.add_section("Pitching Leaderboards")
    l.add_subsection("WAR")
    l.start_table('rllrrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l', 'era',
                   'ip', 'k/9','bb/9','hr/9', 'whip', 'gb%'])
    l.add_rows(pit_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}',
                        '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}',
                        '{:.1f}', '{:.1f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("WAR - Relivers")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l','era',
                   'ip','k/9','bb/9','hr/9','gb%'])
    l.add_rows(rel_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}',
                        '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}',
                        '{:.1f}', '{:.1f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("ERA")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'w', 'l',
                   'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(era_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}',
                        '{:.0f}', '{:.2f}', '{:.1f}', '{:.2f}',
                        '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("ELO Ratings")
    l.start_table('rrlrrr')
    l.add_headers(['', 'Rating', 'Team', 'div%', 'post%', 'ws%'])
    l.add_rows(elo_df, ['{:.0f}', '{:.0f}', '', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.footer()
    l.make_pdf()

