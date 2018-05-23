import scraper
import fangraphs
import baseballreference
import datetime
import utils
import argparse

def leaderboard(state, key):
    s = scraper.Scraper()

    tid = 0 # Scrape all teams for now, add individual teams later if needed
    year = datetime.date.today().strftime('%Y')

    url = """http://www.fangraphs.com/leaders.aspx?pos=all&stats={0}\
             &lg=all&qual=0&type=8&season={1}\
             &month=0&season1={1}\
             &ind=0&team={2}&page=1_1000"""\
             .format(state, year, tid)\
             .replace(' ', '')

    s.scrape(url, key, 'fangraphs', 'fangraphs', 24*60*60)

def schedule(team, year, key):
    s = scraper.Scraper()
    url = "http://www.baseball-reference.com/teams/{}/{}-schedule-scores.shtml".format(team, year)
    s.scrape(url, key, 'baseballreference', 'schedule', 24*60*60)

if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y-%m-%d')
    current_year = today.split('-')[0]

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--team', default='NYY')
    parser.add_argument('-d', '--date', default=today)
    args = parser.parse_args()

    year = args.date.split('-')[0]

    s = scraper.Scraper()

    # scrape schedule
    schedule(args.team, year, '{}_schedule'.format(args.team))

    # scrape leaderboards
    leaderboard('bat', 'fangraphs_bat_leaderboard')
    leaderboard('pit', 'fangraphs_pit_leaderboard')

    print(s.get_key('fangraphs_bat_leaderboard'))





