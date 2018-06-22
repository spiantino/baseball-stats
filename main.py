import argparse
import datetime
import pandas as pd
from collections import defaultdict, Counter

import scrape
from latex import Latex
from tablebuilder import TableBuilder
from dbclasses import Player, Game, Team

def make_pdf(team, date, home, away, summary, pitchers, starters, bench,
             bullpen, standings, history, bat_df, hr_df, rbi_df,
             pit_df, era_df, rel_df, elo_df, pit_hist, last_week_bp,
             series_table, gb):

    l = Latex("{}-{}.tex".format(team, date))
    l.header()
    l.title(summary, pitchers)

    l.start_table('lcclrrrrrrrrr')
    l.add_headers(['Team', 'R/L', '#', 'Name', 'war', 'w', 'l', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(pitchers, ['', '', '{:.0f}', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.2f}', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("{} Lineup".format(away))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash', 'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[0], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    if bench:
        l.add_divider()
        l.add_rows(bench[0], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("{} Lineup".format(home))
    l.start_table('lcclrcrrrrr')
    l.add_headers(['', 'Pos', '#', 'Name', 'war', 'slash', 'hr', 'rbi', 'sb', 'owar', 'dwar'])
    l.add_rows(starters[1], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    if bench:
        l.add_divider()
        l.add_rows(bench[1], ['{:.0f}', '', '{:.0f}', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("Standings")
    l.start_multicol(2)
    for table in standings:
        l.start_table('lrrcccrr')
        l.add_headers([table.columns[0], 'w', 'l', 'l10', 'gb', 'strk', 'home', 'away'])
        l.add_rows(table, ['', '{:.0f}', '{:.0f}', '', '{:.0f}', '', '{:.3f}', '{:.3f}'])
        l.end_table()
    l.end_multicol()

    l.page_break()
    l.add_subsection("{} Bullpen".format(away))
    l.start_table('lcrrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%', 'days'])
    l.add_rows(bullpen[0], ['', '{:.0f}', '{:.1f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.0f}'])
    l.end_table()

    l.add_subsection("{} Bullpen".format(home))
    l.start_table('lcrrrrrrrr')
    l.add_headers(['Name', '#', 'war', 'sv', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(bullpen[1], ['', '{:.0f}', '{:.1f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.add_section("{} Recent Starts".format(args.team))
    l.start_table('lclrrrrrrrr')
    l.add_headers(['Date', 'Opp', 'Starter', 'ip', 'h', 'r', 'er', 'bb', 'k', 'gs'])
    l.add_rows(pit_hist, ['', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}', '{:.0f}'])
    l.end_table()

    l.page_break()
    l.start_multicol(2)
    l.add_subsection("{} Game Log".format(away))
    l.start_table('lrlccc')
    l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    l.add_rows(history[1], ['', '', '', '', '', ''])
    l.end_table()

    l.add_subsection("{} Game Log".format(home))
    l.start_table('lrlccc')
    l.add_headers(['Date', 'Time', 'Opp', ' ', 'Score', 'gb'])
    l.add_rows(history[0], ['', '', '', '', '', ''])
    l.end_table()
    l.end_multicol()

    l.add_section("Batting Leaderboards")
    l.start_table('rllrcrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'slash', 'hr', 'rbi', 'sb', 'bb%', 'k%', 'babip', 'owar', 'dwar'])
    l.add_rows(bat_df, ['{:.0f}', '', '', '{:.1f}', '', '{:.0f}', '{:.0f}', '{:.0f}', '{:.1f}', '{:.1f}', '{:.3f}', '{:.1f}', '{:.1f}'])
    l.end_table()

    l.start_multicol(2)
    l.add_subsection("HR")
    l.start_table('llrr')
    l.add_headers(['Name','Team', 'hr', '#'])
    l.add_rows(hr_df, ['', '', '{:.0f}', '{:.0f}'])
    l.end_table()

    l.add_subsection("RBI")
    l.start_table('llrr')
    l.add_headers(['Name','Team', 'rbi', '#'])
    l.add_rows(rbi_df, ['', '', '{:.0f}', '{:.0f}'])
    l.end_table()
    l.end_multicol()

    l.add_section("Pitching Leaderboards")
    l.add_subsection("WAR")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l','era','ip','k/9','bb/9','hr/9','gb%'])
    l.add_rows(pit_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("WAR - Relivers")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['','Name','Team','war','w','l','era','ip','k/9','bb/9','hr/9','gb%'])
    l.add_rows(rel_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.1f}', '{:.2f}'])
    l.end_table()

    l.add_subsection("ERA")
    l.start_table('rllrrrrrrrrr')
    l.add_headers(['', 'Name', 'Team', 'war', 'w', 'l', 'era', 'ip', 'k/9', 'bb/9', 'hr/9', 'gb%'])
    l.add_rows(era_df, ['{:.0f}', '', '', '{:.1f}', '{:.0f}', '{:.0f}', '{:.2f}', '{:.1f}', '{:.2f}', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.add_section("ELO Ratings")
    l.start_table('rrlrrr')
    l.add_headers(['', 'Rating', 'Team', 'div%', 'post%', 'ws%'])
    l.add_rows(elo_df, ['{:.0f}', '{:.0f}', '', '{:.2f}', '{:.2f}', '{:.2f}'])
    l.end_table()

    l.footer()
    l.make_pdf()


def get_pitch_counts(player):
    """
    Return counts of all pitch types for input player
    """
    p = Player(name=player)
    team = p.get_stat('Team')

    g = Game()
    dates = g.find_pitch_dates(player, team)

    counts =  Counter()

    last_date = None
    for date in dates:

        # Check if game is a double header
        idx = 1 if date == last_date else 0

        g = Game()
        g.query_game_preview_by_date(team, date, idx)
        g.parse_pitch_types()
        game_counts = g._pit_counts[player]
        counts += game_counts

        last_date = date

    return counts


def scrape_update(home, away, year):
    print("Gathering game previews...")
    scrape.game_previews()

    print("Scraping past boxscores...")
    scrape.boxscores(date='all')

    print("Scraping batting leaderboard...")
    scrape.fangraphs(state='bat', year=year)

    print("Scraping pitching leaderboard...")
    scrape.fangraphs(state='pit', year=year)

    print("Scraping league standings...")
    scrape.standings()

    print("Scraping schedule, roster, pitch logs, injuries, transactions...")
    for team in [home, away]:
        scrape.schedule(team)
        scrape.pitching_logs(team, year)
        scrape.current_injuries(team)
        scrape.transactions(team, year)
        scrape.forty_man(team, year)

    scrape.league_elo()


if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    g = Game()
    g.query_game_preview_by_date(team=args.team, date=args.date)
    g.parse_all()

    opp  = g._opp

    home = g._game['home']
    away = g._game['away']
    year = g._date.split('-')[0]

    # scrape_update(g._team, g._opp, year)

    tb=TableBuilder(g)

    summary = tb.summary_info()
    pitchers = tb.starting_pitchers()
    starters, bench = tb.rosters()
    bullpen = tb.bullpen()
    standings = tb.standings()
    history = tb.game_history()
    bat_df = tb.bat_leaders(stat='WAR', n=30)
    hr_df  = tb.bat_leaders(stat='HR',  n=10)
    rbi_df = tb.bat_leaders(stat='RBI', n=10)
    pit_df = tb.pit_leaders(stat='WAR', n=30, role='starter')
    era_df = tb.pit_leaders(stat='ERA', n=10, role='starter')
    rel_df = tb.pit_leaders(stat='WAR', n=10, role='reliever')
    elo_df = tb.elo()
    pit_hist = tb.pitcher_history()
    last_week_bp = tb.previous_week_bullpen()
    series_table = tb.series_results()
    gb = tb.games_behind(args.team, opp)

    print(summary)
    print(pitchers)
    print(starters)
    print(bench)
    print(bullpen)
    print(standings)
    print(history)
    print(bat_df)
    print(hr_df)
    print(rbi_df)
    print(pit_df)
    print(era_df)
    print(rel_df)
    print(elo_df)
    print(pit_hist)
    print(last_week_bp)
    print(series_table)
    print(gb)

    make_pdf(args.team, args.date, home, away,
             summary, pitchers, starters,
             bench, bullpen, standings,
             history, bat_df, hr_df, rbi_df,
             pit_df, era_df, rel_df, elo_df,
             pit_hist, last_week_bp, series_table, gb)

