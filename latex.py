import pandas as pd
import jinja2
import tempfile
import subprocess
import os

class Latex:
	def __init__(self, filename):
		self._filename = filename
		self._f = open(filename, "w")

	def tex_escape(s):
		if isinstance(s, str):
			s = s.replace('%', '\\%')
			s = s.replace('#', '\\#')
			s = s.replace('_', ' ')
		return s

	def pd_to_tabulated(self, pd):
		
		column_def = ""
		rows = pd.to_dict(orient='records')

		if len(rows) == 0:
			column_def = "c"
			rows = [{" " : "None"}]
		else:
			for key, value in rows[0].items():
				if isinstance(value, str):
					column_def += "l"
				else:
					column_def += "r"

			for i, row in enumerate(rows):
				rows[i] = {Latex.tex_escape(k): Latex.tex_escape(v) for k, v in row.items()}

		t = jinja2.Template(
			r"""
			\begin{tabular}{@{}{{ column_def }}@{}}
			{% for c in columns %}\textbf{ {{- c -}} }{% if not loop.last %} & {% endif %}{% endfor %} \\ \midrule
			{% for row in data %}{% for k, v in row.items() %} {{ v }} {% if not loop.last %}&{% endif %}{% endfor %} \\
			{% if not loop.last %}\hdashline{% endif %}{% endfor %}
			\bottomrule
			\end{tabular}
			""")

		return t.render(column_def = column_def,
						columns = rows[0].keys(),	
						data = rows)

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
				\setmainfont[Scale=0.85]{TeX Gyre Schola} % Incredible font inside latex


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

		self._f.write(self.pd_to_tabulated(summary['pit_df']))

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


	def add_table(self, pd):
		self._f.write(self.pd_to_tabulated(pd))


	def footer(self):
		self._f.write(r"""
			\end{document}
			""")

	def make_pdf(self):
		self._f.close()
		subprocess.call(['xelatex', self._filename])
	    
	    


