from pymongo import MongoClient
from bs4 import BeautifulSoup, Comment
from tqdm import tqdm
import requests
import argparse
import json
import datetime
import pickle
import inspect

client = MongoClient('localhost', 27017)
db = client.mlbDB

def open_url(url):
    page = requests.get(url)
    return BeautifulSoup(page.text, "html.parser")


def convert_name(name, how):
    """
    Convert between abbreviation
    and full team name
    """
    full2abbr = {
                  'angels'       : 'laa',
                  'astros'       : 'hou',
                  'athletics'    : 'oak',
                  'blue jays'    : 'tor',
                  'braves'       : 'atl',
                  'brewers'      : 'mil',
                  'cardinals'    : 'stl',
                  'cubs'         : 'chc',
                  'diamondbacks' : 'ari',
                  'dodgers'      : 'lad',
                  'giants'       : 'sfg',
                  'indians'      : 'cle',
                  'mariners'     : 'sea',
                  'marlins'      : 'mia',
                  'mets'         : 'nym',
                  'nationals'    : 'wsn',
                  'orioles'      : 'bal',
                  'padres'       : 'sdp',
                  'phillies'     : 'phi',
                  'pirates'      : 'pit',
                  'rangers'      : 'tex',
                  'rays'         : 'tbr',
                  'red sox'      : 'bos',
                  'reds'         : 'cin',
                  'rockies'      : 'col',
                  'royals'       : 'kcr',
                  'tigers'       : 'det',
                  'twins'        : 'min',
                  'white sox'    : 'chw',
                  'yankees'      : 'nyy'
                  }
    abbr2full = {k:v for v,k in full2abbr.items()}

    name = name.lower()

    if name == 'all':
        return name

    elif how == 'full':
        if len(name) == 3:
            return abbr2full[name].upper()
        else:
            return name

    elif how == 'abbr':
        if len(name) != 3:
            return full2abbr[name].upper()
        else:
            return name.upper()


def fangraphs(state, year):
    """
    Scrape data from fangraphs.com
    """
    # team = convert_name(name=team, how='full') if all_=='off' else 'all'
    # team_id = {
    #            'all'          : 0,
    #            'angels'       : 1,
    #            'astros'       : 21,
    #            'athletics'    : 10,
    #            'blue jays'    : 14,
    #            'braves'       : 16,
    #            'brewers'      : 23,
    #            'cardinals'    : 28,
    #            'cubs'         : 17,
    #            'diamondbacks' : 15,
    #            'dodgers'      : 22,
    #            'giants'       : 30,
    #            'indians'      : 5,
    #            'mariners'     : 11,
    #            'marlins'      : 20,
    #            'mets'         : 25,
    #            'nationals'    : 24,
    #            'orioles'      : 2,
    #            'padres'       : 29,
    #            'phillies'     : 26,
    #            'pirates'      : 27,
    #            'rangers'      : 13,
    #            'rays'         : 12,
    #            'red sox'      : 3,
    #            'reds'         : 18,
    #            'rockies'      : 19,
    #            'royals'       : 7,
    #            'tigers'       : 6,
    #            'twins'        : 8,
    #            'white sox'    : 4,
    #            'yankees'      : 9
    #            }

    # team = team.lower()
    # tid = team_id[team]

    tid = 0 # Scrape all teams for now, add individual teams later if needed

    url = """http://www.fangraphs.com/leaders.aspx?pos=all&stats={0}\
             &lg=all&qual=0&type=8&season={1}\
             &month=0&season1={1}\
             &ind=0&team={2}&page=1_1000"""\
             .format(state, year, tid)\
             .replace(' ', '')

    soup = open_url(url)

    # Extract column headers
    thead = soup.find('thead')
    cols = [x.text for x in thead.find_all('th')]

    # Extract stats from table bdoy
    tbody = soup.find_all('tbody')[-1]
    all_rows = tbody.find_all('tr')
    all_row_data = [x.find_all('td') for x in all_rows]

    for row in tqdm(all_row_data):
        row_data = [x.text for x in row]
        player = row_data[1]
        db_data = {k:v for k,v in zip(cols, row_data)}

        # Rename common keys with batting or pitching prefixes
        rank = '{}_rank'.format(state)
        db_data[rank] = db_data.pop('#')

        war = '{}_WAR'.format(state)
        db_data[war] = db_data.pop('WAR')

        games = '{}_G'.format(state)
        db_data[games] = db_data.pop('G')

        # Store type as numeric if possible
        for key in db_data.keys():
            try:
                db_data[key] = float(db_data[key])
            except:
                continue

        # Insert row into database
        db.Players.update({'Name': player},
                          {'$set' : {year : db_data}}, upsert=True)


def standings():
    """
    Scrape MLB standings or Yankees schedule
    from baseball-reference.com
    """
    url = "http://www.baseball-reference.com/leagues/MLB-standings.shtml"
    soup = open_url(url)

    # Scrape division identifier data
    div_data, gb_data = {}, {}
    divs = ['E', 'C', 'W']
    for div in divs:
        table = soup.find_all('div', {'id' : 'div_standings_{}'.format(div)})

        trows = [x.find_all('tr') for x in table]
        trows_flat = [x for y in trows for x in y]

        teams_html = [x.find('a') for x in trows_flat if x.find('a')]
        team_names = [x['href'].split('/')[2] for x in teams_html]

        gbs = [x.find('td', {'data-stat' : 'games_back'}).text
               for x in trows_flat if x.find('td')]

        div_dict = {k : div for k in team_names}
        div_data.update(div_dict)

        gb_dict  = {k : v for k,v in zip(team_names, gbs)}
        gb_data.update(gb_dict)

    # Scrape full league standings
    comment = soup.find_all(string=lambda text: isinstance(text, Comment))
    comment_html = [x for x in comment if '<td' in x][-1].string

    comment_soup = BeautifulSoup(comment_html, "html.parser")

    # Extract table column headers
    thead = comment_soup.find('thead')
    cols = [x.text.replace('.', '') for x in thead.find_all('th')]

    # Extract table body
    tbody = comment_soup.find('tbody')
    trows = tbody.find_all('tr')

    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]

        # Skip last row (league averages)
        if row_data[0]:
            team = row_data[1]
            db_data = {k:v for k,v in zip(cols, row_data)}

            # Add division and gb information
            division = '{}-{}'.format(db_data['Lg'], div_data[team])
            games_behind = gb_data[team]
            db_data.update({'div' : division})
            db_data.update({'gb' : games_behind})

            # Insert row into database
            db.Teams.update({'Tm' : team}, db_data, upsert=True)

def schedule(team):
    """
    Scrape yankees schedule with results
    from baseball-reference.com
    """
    name = convert_name(team, how='abbr')
    url = "http://www.baseball-reference.com/teams/{}/2018-schedule-scores.shtml".format(name)

    soup = open_url(url)
    table = soup.find('table', {'id' : 'team_schedule'})

    # Extract schedule columns
    thead = table.find('thead')
    cols = [x.text.replace('\xa0', 'Field') for x in thead.find_all('th')]
    upcoming_cols = cols[:6] + ['Date']

    # Extract schedule data
    tbody = soup.find('tbody')
    trows = tbody.find_all('tr')

    # Throw out rows that are duplicates of column headers
    trows = [x for x in trows if 'Gm#' not in x.text]

    # Clear existing Schedule document
    db.Teams.update({'Tm' : name}, {'$set': {'Schedule' : []}})

    # Extract schedule data one row at a time
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]

        # Past game
        if row_data[2] == 'boxscore':
            game_num = row_data[0]
            db_data = {k:v for k,v in zip(cols, row_data)}

        # Upcoming game
        elif row_data[2] == 'preview':
            row_data = row_data[:7]
            game_num = row_data[0]
            db_data = {k:v for k,v in zip(upcoming_cols, row_data)}

        # Insert row into database
        db.Teams.update({'Tm' : name},
                        {'$push': {'Schedule': db_data}})


def pitching_logs(team, year):
    """
    Scrape pitching logs from
    baseball-reference.com
    """
    team = convert_name(name=team, how='abbr')
    url = "http://www.baseball-reference.com/teams/tgl.cgi?team={}&t=p&year={}".format(team, year)

    soup = open_url(url)

    table = soup.find_all('div', {'class' : 'table_outer_container'})[-1]

    # Extract column headers
    cols = [x.text for x in table.find_all('th', {'scope' : 'col'})]

    # Extract body of pitching logs table
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')

    # # Clear existing Pitlog document
    db_array = 'Pitlog.{}'.format(year)
    db.Teams.update({'Tm' : team},
                    {'$set': {db_array : []}})

    # Extract pitching logs and push to databse
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}

        # Insert row indo database
        db.Teams.update({'Tm' : team},
                        {'$push': {db_array : db_data}})


def forty_man(team, year):
    """
    Extract 40-man roster from
    baseball-reference.com
    """
    team = convert_name(name=team, how='abbr')
    url = "http://www.baseball-reference.com/teams/{}/{}-roster.shtml"\
                                                    .format(team, year)
    soup = open_url(url)

    table = soup.find('table', {'id' : 'the40man'})

    # Extract column headers
    thead = table.find('thead')
    cols = [x.text for x in thead.find_all('th')]

    # Extract body of fort man table
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')

    # Clear existing Fortyman document
    db_array = 'Fortyman.{}'.format(year)
    db.Teams.update({'Tm' : team},
                    {'$set': {db_array : []}})

    # Extract forty-man roster and push to database
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db.Teams.update({'Tm' : team},
                        {'$push': {db_array : db_data}})


def current_injuries(team):
    """
    Extract current injuries table
    from baseball-reference.com
    """
    current_year = datetime.date.today().strftime('%Y')

    team = convert_name(name=team, how='abbr')

    url = "http://www.baseball-reference.com/teams/{}/{}.shtml"\
                                            .format(team, current_year)
    soup = open_url(url)

    # Data is stored in html comment
    comment = soup.find_all(string=lambda text: isinstance(text, Comment))
    comment_html = [x for x in comment if 'Injuries Table' in x][-1].string

    table = BeautifulSoup(comment_html, "html.parser")

    # Extract column headers
    thead = table.find('thead')
    cols = [x.text for x in thead.find_all('th')]

    # Extract body from injuries table
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')

    # Clear existing injuries document
    db.Teams.update({'Tm' : team},
                    {'$set': {'Injuries' : []}})

    # Extract injuries table and push to database
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db.Teams.update({'Tm' : team},
                        {'$push' : {'Injuries' : db_data}})


def transactions(team, year):
    """
    Extract transations from
    http://mlb.mlb.com/mlb/transactions
    """
    team = convert_name(name=team, how='full')
    team_id = {
               'angels'       : 108,
               'astros'       : 117,
               'athletics'    : 133,
               'blue jays'    : 141,
               'braves'       : 144,
               'brewers'      : 158,
               'cardinals'    : 138,
               'cubs'         : 112,
               'diamondbacks' : 109,
               'dodgers'      : 119,
               'giants'       : 137,
               'indians'      : 114,
               'mariners'     : 136,
               'marlins'      : 146,
               'mets'         : 121,
               'nationals'    : 120,
               'orioles'      : 110,
               'padres'       : 135,
               'phillies'     : 143,
               'pirates'      : 134,
               'rangers'      : 140,
               'rays'         : 139,
               'red sox'      : 111,
               'reds'         : 113,
               'rockies'      : 115,
               'royals'       : 118,
               'tigers'       : 116,
               'twins'        : 142,
               'white sox'    : 145,
               'yankees'      : 147,
               }
    tid = team_id[team]

    current_year = datetime.date.today().strftime('%Y')

    if str(year) == current_year:
        today = datetime.date.today().strftime('%Y%m%d')
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date={}0101&end_date={}&team_id={}".format(year, today, tid)
    else:
        url = "http://mlb.mlb.com/lookup/json/named.transaction_all.bam?start_date={0}0101&end_date={0}1231&team_id={1}".format(year, tid)

    # Open and read json object
    res = requests.get(url).text
    j = json.loads(res)

    # Name transactions array by year in database
    db_array = 'Transactions.{}'.format(year)

    # Clear existing Transactions document
    db.Teams.update({'Tm' : team},
                    {'$set': {db_array : []}})

    # Add Transactions json data to database
    db.Teams.update({'Tm' : team},
                    {'$push' : {db_array : j}})

def boxscores(year):
    url = 'https://www.baseball-reference.com/leagues/MLB/{}-schedule.shtml'.format(year)

    soup = open_url(url)

    boxes = [x for x in soup.find_all('a', href=True) if x.text=='Boxscore']
    previews = [x for x in soup.find_all('a', href=True) if x.text=='Preview']


def boxscores(date):
    """
    Extract all boxscores
    """
    # Get yesterday's date and boxscores
    today = datetime.date.today().strftime('%m/%d/%Y')

    # If date arg is today (default), look for yesterdays boxes
    if date == today:
        date = ((datetime.date.today() -
                      datetime.timedelta(1))
                      .strftime('%Y-%m-%d'))
        y, m, d = date.split('-')
    else:
        y, m, d = date.split('-')

    url = "http://www.baseball-reference.com/boxes/?year={}\
           &month={}\
           &day={}"\
           .format(y,m,d)\
           .replace(' ', '')

    soup = open_url(url)

    game_urls = [x.a['href'] for x in
                 soup.find_all('td', {'class' : 'right gamelink'})]

    # Collect boxscore stats on each game
    for game in tqdm(game_urls):
        url = 'http://www.baseball-reference.com' + game
        soup = open_url(url)

        # Store HOME and AWAY team names
        away, home = soup.find_all('h2')[0].text, soup.find_all('h2')[1].text
        teams = (away, home)

        # Create new Game document
        gid = game.split('/')[-1].split('.')[0]
        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date},
                        {'$set' : {'gid' : gid,
                                   'date' : date,
                                   'home' : home,
                                   'away' : away}}, upsert=True)

        # Extract summary stats
        summary = soup.find('table', {'class' : 'linescore'})

        thead = summary.find('thead')
        cols = [x.text for x in thead.find_all('th')][1:]
        cols[0] = 'Team'

        tbody = summary.find('tbody')
        trows = tbody.find_all('tr')

        # Push summary stats to database
        for row in trows:
            row_data = [x.text for x in row.find_all('td')][1:]
            db_data  = {k:v for k,v in zip(cols, row_data)}
            db.Games.update({'gid' : gid},
                            {'$push': {'summary' : db_data}})

        # Extract batting box score
        comment = soup.find_all(string=lambda text: isinstance(text,Comment))
        bat_tables = [x for x in comment if '>Batting</th>' in x]

        for table in zip(teams, bat_tables):
            team = table[0].replace('.', '')
            bat = BeautifulSoup(table[1], "html.parser")

            # Extract column headers
            thead = bat.find('thead')
            cols = [x for x in thead.find('tr').text.split('\n') if x]

            # Extract Team Totals
            tfoot = bat.find('tfoot')
            row_data = [x.text for x in
                        tfoot.find_all(lambda tag: tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_array = '{}.batting'.format(team)
            db.Games.update({'gid' : gid},
                            {'$set' : {db_array : [db_data]}})

            # Extract stats on individual batters
            tbody = bat.find('tbody')
            trows = tbody.find_all('tr')
            for row in trows:
                try:
                    player = row.find('a').text
                except:
                    continue
                stats = [x.text for x in
                         row.find_all(lambda tag: tag.has_attr('data-stat'))]
                stats[0] = player
                db_data = {k:v for k,v in zip(cols, stats)}
                db_array = '{}.batting'.format(team)
                db.Games.update({'gid' : gid},
                                {'$push' : {db_array : db_data}})


        # Extract pitching box score
        pit_tables  = [x for x in comment if '>Pitching</th>' in x][0]
        pit = BeautifulSoup(pit_tables, "html.parser")

        # Extract column headers
        thead = pit.find('thead')
        cols = [x for x in thead.find('tr').text.split('\n') if x]

        # Extract Team Totals
        tfoots = pit.find_all('tfoot')
        for foot in zip(teams, tfoots):
            team = foot[0].replace('.', '')
            row_data = [x.text for x in
                        foot[1].find_all(lambda tag:
                                                tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_array = '{}.pitching'.format(team)
            db.Games.update({'gid' : gid},
                            {'$set' : {db_array : [db_data]}})

        # Extract stats on individual pitchers
        tbodies = pit.find_all('tbody')
        for tbody in zip(teams, tbodies):
            team = tbody[0].replace('.', '')
            trows = tbody[1].find_all('tr')
            for row in trows:
                player = row.find('th').text.split(',')[0]
                stats  = [x.text for x in row.find_all('td')]
                stats.insert(0, player)
                db_data = {k:v for k,v in zip(cols, stats)}
                db_array = '{}.pitching'.format(team)
                db.Games.update({'gid' : gid},
                                {'$push' : {db_array : db_data}})


def game_preview():
    """
    Collect data on upcomming game
    from mlb.com/gameday
    """

    date = datetime.date.today().strftime('%Y-%m-%d')

    url = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}'\
                                                        .format(date)

    res = requests.get(url).text
    schedule_data = json.loads(res)

    games_data = schedule_data['dates'][0]['games']

    # Gather all game urls
    game_urls = [game['link'] for game in games_data]

    # Collect data on all upcoming games
    for url in tqdm(game_urls):
        game_url = 'https://statsapi.mlb.com' + url
        res = requests.get(game_url).text
        game_data = json.loads(res)

        # Check if game has already started (json is structured differently)
        state = game_data['gameData']['status']['detailedState']

        # Get HOME and AWAY team names
        if state == 'Scheduled':
            home = game_data['gameData']['teams']['home']['abbreviation']
            away = game_data['gameData']['teams']['away']['abbreviation']
        else:
            home = game_data['gameData']['teams']['home']['name']['abbrev']
            away = game_data['gameData']['teams']['away']['name']['abbrev']

        # Push to database
        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date},
                        {'$set': {'preview': []}})

        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date},
                         {'$push': {'preview': game_data}}, upsert=True)


def league_elo():
    """
    – Rank
    – Team
    – Rating
    – Playoffs %
    – Division %
    – World Series %
    """
    url = 'https://projects.fivethirtyeight.com/2018-mlb-predictions/'
    soup = open_url(url)

    tbody = soup.find('tbody')
    trows = tbody.find_all('tr')

    cols = ['elo_rating',
            'playoff_pct',
            'division_pct',
            'worldseries_pct']

    for row in trows:
        team = row['data-str']
        rating = float(row.find('td', {'class' : 'num rating'})['data-val'])
        pcts = [float(x['data-val'])
                for x in row.find_all('td', {'class': 'pct'})]

        row_data = [rating] + pcts
        db_data  = {k:v for k,v in zip(cols, row_data)}

        # Clear existing elo document
        tm = convert_name(name=team, how='abbr')
        db.Teams.update({'Tm' : tm}, {'$set': {'elo' : []}})

        db.Teams.update({'Tm' : tm},
                        {'$push': {'elo' : db_data}})




if __name__ == '__main__':
    boxscores('2018-05-06')
    # schedule('red sox')
    # game_preview()
    # fangraphs('bat', '2018')
    # fangraphs('pit', '2018')
    # league_elo()
    # forty_man(team='NYY', year=2018)

    # def run(fn, team, year, date):
    #     arglen = len(inspect.getargspec(fns[fn])[0])

    #     if fn == 'bat_leaders':
    #         fangraphs(state='bat', year=year)
    #     elif fn =='pit_leaders':
    #         fangraphs(state='pit',  year=year)
    #     elif fn == 'boxscores':
    #         boxscores(date=date)
    #     elif arglen == 2:
    #         fns[fn](team=team, year=year)
    #     elif arglen == 1:
    #         fns[fn](team=team)
    #     elif arglen == 0:
    #         fns[fn]()

    # if args.function == 'master':
    #     year_ = args.date.split('/')[-1]
    #     t1, t2 = master(args.team, args.date)

    #     for fn in fns.keys():
    #         if fn == 'forty_man':
    #             forty_man(t1, year_)
    #             forty_man(t2, year_)
    #         elif fn == 'preview':
    #             game_preview(args.team, args.date)
    #         else:
    #             run(fn, args.team, year_)
    # else:
    #     run(args.function, args.team, args.year, args.date)
