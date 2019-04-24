from bs4 import BeautifulSoup, Comment
from tqdm import tqdm
import requests
import argparse
import re
import json
import datetime
import pickle
import inspect
import os
import pandas as pd
from io import StringIO, BytesIO
from PIL import Image
from collections import defaultdict
from urllib.parse import unquote

from selenium.webdriver.chrome.options import Options
from selenium import webdriver

from dbcontroller import DBController
from conflictresolver import ConflictResolver
from utils import open_url, convert_name, parse_types
from dbclasses import Game

# No push methods in DBController, so do it manually for now
dbc = DBController()
db = dbc._db


def fangraphs(state, year):
    """
    Scrape data from fangraphs.com
    """
    tid = 0 # Scrape all teams for now, add individual teams later if needed

    stat_groups = {'Dashboard' : 8,
                   'Standard'  : 0,
                   'Advanced'  : 1}

    data = {}
    for val in stat_groups.values():

        url = """http://www.fangraphs.com/leaders.aspx?pos=all&stats={0}\
                 &lg=all&qual=0&type={1}&season={2}\
                 &month=0&season1={2}\
                 &ind=0&team={3}&page=1_1000"""\
                 .format(state, val, year, tid)\
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
            player_data = {k:v for k,v in zip(cols, row_data)}

            # Convert team name to abbreviation
            try:
                player_data['Team'] = convert_name(player_data['Team'])
            except:
                player_data['Team'] = None

            player_data = parse_types(player_data)

            if player in data.keys():
                data[player].update(player_data)
            else:
                data[player] = player_data

    db_data = [pstats for pstats in data.values()]

    db_path = 'fg.{}.{}'.format(state, year)

    for pdata in db_data:
        db.Players.update_many({'Name' : pdata['Name']},
                               {'$set' : {db_path : pdata}}, upsert=True)

        if year == dbc._year:
            db.Players.update_many({'Name' : pdata['Name']},
                                   {'$set' : {'Team' : pdata['Team']}})


def fangraphs_splits(year):

    # Set up headless chrome
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    browser = webdriver.Chrome(chrome_options=options)
    browser.implicitly_wait(20)


    # 5 is for left handed batters, 6 for right handed batters
    for hand in [5, 6]:
        url = """https://www.fangraphs.com/leaderssplits.aspx?splitArr={0}\
               &strgroup=season&statgroup=1&startDate={1}-03-01\
               &endDate={1}-11-01&filter=&position=P&statType=player\
               &autoPt=true&players=&sort=19,-1&pg=0"""\
               .format(hand, year).replace(' ', '')

        browser.get(url)
        xpath = '//*[@id="react-drop-test"]/div[2]/a'
        csv_btn = browser.find_element_by_xpath(xpath)
        csv = unquote(csv_btn.get_attribute('href')\
                    .replace('data:application/csv;charset=utf-8,', ''))
        df = pd.read_csv(StringIO(csv))

        # Push one row at a time into database
        df_data = df.to_dict(orient='index')
        for key in tqdm(df_data.keys()):
            name = df_data[key]['Name']
            season = df_data[key]['Season']
            player_data = {k:v for k,v in df_data[key].items()
                               if k not in ['Name', 'Season']}

            handstr = 'vLHH' if hand == 5 else 'vRHH'
            db_path = 'fg.{}.{}'.format(handstr, season)

            db.Players.update_one({'Name' : name},
                                  {'$set' : {db_path : player_data}})

    browser.quit()


def standings():
    """
    Scrape MLB standings from baseball-reference.com
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

            # Store int/float when possible
            db_data = parse_types(db_data)

            # Insert row into database
            db.Teams.update({'Tm' : team}, {'$set': db_data}, upsert=True)


def schedule(team, year=None):
    """
    Scrape yankees schedule with results
    from baseball-reference.com
    """
    today = datetime.date.today().strftime('%Y-%m-%d')
    year = today.split('-')[0] if not year else year

    name = convert_name(team, how='abbr')
    url = "http://www.baseball-reference.com/teams/{}/{}-schedule-scores.shtml".format(name, year)

    soup = open_url(url)
    table = soup.find('table', {'id' : 'team_schedule'})

    # Extract schedule columns
    thead = table.find('thead')
    cols = [x.text.replace('\xa0', 'Field').replace('.', '') for x in thead.find_all('th')]
    upcoming_cols = cols[:6] + ['Time']

    # Extract schedule data
    tbody = soup.find('tbody')
    trows = tbody.find_all('tr')

    # Throw out rows that are duplicates of column headers
    trows = [x for x in trows if 'Gm#' not in x.text]

    # Clear existing Schedule document
    db.Teams.update_one({'Tm' : name}, {'$set': {'Schedule' : []}})

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

        db_data = parse_types(db_data)

        # Insert row into database
        db.Teams.update_one({'Tm' : name},
                            {'$push': {'Schedule': db_data}})


def pitching_logs(team, year=None):
    """
    Scrape pitching logs from
    baseball-reference.com
    """
    today = datetime.date.today().strftime('%Y-%m-%d')
    year = today.split('-')[0] if not year else year

    team = convert_name(name=team, how='abbr')
    url = "http://www.baseball-reference.com/teams/tgl.cgi?team={}&t=p&year={}".format(team, year)

    soup = open_url(url)

    table = soup.find_all('div', {'class' : 'table_outer_container'})[-1]

    # Extract column headers
    cols = [x.text for x in table.find_all('th', {'scope' : 'col'})]

    # Extract body of pitching logs table
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')

    # Clear existing Pitlog document
    db_array = 'Pitlog.{}'.format(year)
    db.Teams.update_one({'Tm' : team},
                        {'$set': {db_array : []}})

    # Extract pitching logs and push to databse
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db_data = parse_types(db_data)

        # Insert row indo database
        db.Teams.update_one({'Tm' : team},
                            {'$push': {db_array : db_data}})


def forty_man(team, year):
    """
    Extract 40-man roster from
    baseball-reference.com
    """
    team = convert_name(name=team, how='abbr')
    base = "http://www.baseball-reference.com"
    url = base + "/teams/{}/{}-roster.shtml".format(team, year)
    soup = open_url(url)

    table = soup.find('table', {'id' : 'the40man'})

    # Extract column headers and rename blank columns
    thead = table.find('thead')
    cols = [x.text for x in thead.find_all('th')]
    cols[3], cols[4] = 'Country', 'Pos'

    # Extract body of fort man table
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')

    # Clear existing Fortyman document
    db_array = 'Fortyman.{}'.format(year)
    db.Teams.update_one({'Tm' : team},
                        {'$set': {db_array : []}})

    # Extract forty-man roster and push to database
    for row in tqdm(trows):
        bid = row.find('a')['href'].split('=')[-1]
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db_data.update({'bid' : bid})
        db.Teams.update_one({'Tm' : team},
                            {'$push': {db_array : db_data}})

        # Check if player exists in database
        player = db_data['Name']
        exists = dbc.player_exists(player)
        if not exists:
            try:
                print("Scraping br data for {}".format(player))
                br_player_stats(player, team)
            except:
                print("Unable to scrape br data for {}".format(player))


def br_player_stats(name, team):
    brid = dbc.get_player_brid(name, team)
    base = 'https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID='
    url = base + brid

    redirect = requests.get(url).url
    soup = open_url(redirect)

    # Extract Standard Batting/Pitching table
    table = soup.find('div', {'class' : 'table_outer_container'})

    thead = table.find('thead')
    cols = [x.text for x in thead.find_all('th')]
    pit_or_bat = 'pit' if 'Pitching' in table.find('caption').text else 'bat'

    tbody  = table.find('tbody')
    trows = tbody.find_all('tr')

    # Push to Players collection
    for row in trows:
        if row.find('th', {'data-stat' : 'year_ID'}).text:
            row_data = [x.text for x in row.find_all(lambda tag:
                                        tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_data = parse_types(db_data)

            # Skip blank rows and don't collect minor league data
            if not row_data[0] or db_data['Lg'] not in ['AL', 'NL']:
                continue

            db_array = 'br.{}.{}'.format(pit_or_bat, db_data['Year'])
            push = {'{}.{}'.format(db_array, stat) : val
                                         for stat, val in db_data.items()}
            db.Players.update({'Name' : name},
                              {'$set' : push}, upsert=True)

    # Extract Player Value Table - Stored in html comment
    comment = soup.find_all(string=lambda text: isinstance(text, Comment))
    comment_html = [x for x in comment if 'Player Value' in x]

    for c in comment_html:
        table = BeautifulSoup(c.string, "html.parser")

        caption = str(table.find('caption'))
        pit_or_bat = 'pit' if 'Pitching' in caption else 'bat'

        thead = table.find('thead')
        cols = [x.text for x in thead.find_all('th')]

        tbody = table.find('tbody')
        trows = tbody.find_all('tr')

        for row in trows:
            row_data = [x.text for x in
                        row.find_all(lambda tag: tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_data = parse_types(db_data)

            # Skip blank rows and don't collect minor league data
            if not row_data[0] or db_data['Lg'] not in ['AL', 'NL']:
                continue

            db_array = 'br.{}.{}'.format(pit_or_bat, db_data['Year'])
            push = {'{}.{}'.format(db_array, stat) : val
                                         for stat, val in db_data.items()}
            db.Players.update({'Name' : name},
                              {'$set' : push}, upsert=True)


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
    db.Teams.update_one({'Tm' : team},
                        {'$set': {'Injuries' : []}})

    # Extract injuries table and push to database
    for row in trows:
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db_data = parse_types(db_data)
        db.Teams.update_one({'Tm' : team},
                            {'$push' : {'Injuries' : db_data}})


def transactions(team, year):
    """
    Extract transations from
    http://mlb.mlb.com/mlb/transactions
    """
    team = convert_name(name=team, how='abbr').lower()
    team_id = {
               'laa' : 108,
               'hou' : 117,
               'oak' : 133,
               'tor' : 141,
               'atl' : 144,
               'mil' : 158,
               'stl' : 138,
               'chc' : 112,
               'ari' : 109,
               'lad' : 119,
               'sfg' : 137,
               'cle' : 114,
               'sea' : 136,
               'mia' : 146,
               'nym' : 121,
               'wsn' : 120,
               'bal' : 110,
               'sdp' : 135,
               'phi' : 143,
               'pit' : 134,
               'tex' : 140,
               'tbr' : 139,
               'bos' : 111,
               'cin' : 113,
               'col' : 115,
               'kcr' : 118,
               'det' : 116,
               'min' : 142,
               'chw' : 145,
               'nyy' : 147,
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
    db_data = [x for x in j['transaction_all']['queryResults']['row']]

    # Name transactions array by year in database
    db_array = 'Transactions.{}'.format(year)

    # Clear existing Transactions document
    db.Teams.update_one({'Tm' : team.upper()},
                        {'$set': {db_array : []}})

    # Add Transactions json data to database
    for row in db_data:
        db.Teams.update_one({'Tm' : team.upper()},
                            {'$push' : {db_array : row}})


def boxscores(dbc=dbc):
    """
    Extract all boxscores
    """

    # # Delete games with postponed status to avoid conflict
    # dbc.delete_duplicate_game_docs()

    c = ConflictResolver()
    c.run()

    year = datetime.date.today().strftime('%Y')

    url = 'https://www.baseball-reference.com/leagues/MLB/{}-schedule.shtml'\
                                                                .format(year)
    soup = open_url(url)

    # Find links for each game this season
    all_game_urls = [x['href'] for x in soup.find_all('a', href=True)
                                     if x.text=='Boxscore']

    dates = c.get_missing_array_dates('summary')

    # Format dates to match date in url
    datesf = [x.replace('-', '') for x in dates]

    # Filter games by missing dates
    game_urls = [game for game in all_game_urls
                               if any(date in game for date in datesf)]


    # Collect boxscore stats on each game
    for game in tqdm(game_urls):
        url = 'http://www.baseball-reference.com' + game
        soup = open_url(url)

        html_date = ''.join(soup.find('h1').text.split(',')[-2:]).strip()
        date = str(datetime.datetime.strptime(html_date, '%B %d %Y').date())

        tdiv = soup.find_all('div', {'itemprop' : 'performer'})
        away = tdiv[0].find('a', {'itemprop' : 'name'})['href'].split('/')[2]
        home = tdiv[1].find('a', {'itemprop' : 'name'})['href'].split('/')[2]

        away = convert_name(away, how='abbr')
        home = convert_name(home, how='abbr')
        teams = (away, home)

        # Find mlb game id and resolve double headers
        url_id = int(game.split('.')[0][-1])
        games = list(dbc.get_team_game_preview(away, date))
        gnums = [int(game['preview'][0]['gameData']['game']['gameNumber'])
                                                       for game in games]

        # !!! remove try/except here since postponed games are now removed?
        try:
            idx = gnums.index(url_id) if url_id > 0 else 0
        except:
            idx = 0
        try:    #!!! why are some games not added to db in preview scrape?
            gid = games[idx]['gid']
        except:
            print(date, url)
            continue

        # Extract summary stats
        summary = soup.find('table', {'class' : 'linescore'})

        thead = summary.find('thead')
        cols = [x.text for x in thead.find_all('th')][1:]
        cols[0] = 'Team'

        tbody = summary.find('tbody')
        trows = tbody.find_all('tr')

        # Push summary stats to database
        db.Games.update({'gid' : gid},
                        {'$set': {'summary' : []}})

        for row in trows:
            row_data = [x.text for x in row.find_all('td')][1:]
            db_data  = {k:v for k,v in zip(cols, row_data)}
            db.Games.update({'gid' : gid},
                            {'$push': {'summary' : db_data}})

        # Extract batting box score
        comment = soup.find_all(string=lambda text: isinstance(text,Comment))
        bat_tables = [x for x in comment if '>Batting</th>' in x]

        for table in zip(teams, bat_tables):
            team = table[0]
            bat = BeautifulSoup(table[1], "html.parser")

            # Extract column headers
            thead = bat.find('thead')
            cols = [x for x in thead.find('tr').text.split('\n') if x]

            # Extract Team Totals
            tfoot = bat.find('tfoot')
            row_data = [x.text for x in
                        tfoot.find_all(lambda tag: tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_data = parse_types(db_data)
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
                db_data = parse_types(db_data)
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
            db_data = parse_types(db_data)
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
                db_data = parse_types(db_data)
                db_array = '{}.pitching'.format(team)
                db.Games.update({'gid' : gid},
                                {'$push' : {db_array : db_data}})


def get_past_schedule_dates(year=None):
    """
    Return all dates on MLB schedule
    from start of season through today
    """
    today = datetime.date.today().strftime('%Y-%m-%d')
    year = today.split('-')[0] if not year else year

    url = 'https://www.baseball-reference.com/leagues/MLB/{}-schedule.shtml'\
                                                                .format(year)
    soup = open_url(url)

    content = soup.find('div', {'class' : 'section_content'})
    all_dates = [date.text for date in content.find_all('h3')]
    try:
        end = all_dates.index("Today's Games")
    except:
        end = None

    past_dates = all_dates[:end]
    past_dates = [datetime.datetime.strptime(x, '%A, %B %d, %Y')\
                                   .strftime('%Y-%m-%d') for x in past_dates]
    past_dates = set(past_dates).union(set([today]))
    return past_dates


def find_dates_for_update():
    """
    Return list of dates that are missing preview data
    """
    past_schedule_dates = get_past_schedule_dates()
    past_database_dates = dbc.get_past_game_dates()

    dates = past_schedule_dates - past_database_dates

    # Finds all dates without boxscores (including today)
    outdated = set(dbc.find_outdated_game_dates())

    dates = dates.union(outdated)
    dates = sorted(list(dates))
    return dates


def game_previews(dbc=dbc):
    """
    Collect data on upcomming game
    from mlb.com/gameday
    """
    dates = find_dates_for_update()

    url_string = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}'

    urls = [(date, url_string.format(date)) for date in dates]

    print("Gathering game data urls...")
    game_urls = []
    for date, url in tqdm(urls):
        res = requests.get(url).text
        schedule_data = json.loads(res)

        # Skip days where no games are played
        if schedule_data['totalGames'] == 0:
            continue

        games_data = schedule_data['dates'][0]['games']

        # Gather all game urls
        gdata = [(date, game['link'], game['status']['detailedState'])
                                              for game in games_data]
        game_urls += [game for game in gdata]

    # Collect data on all upcoming games
    print("Collecting game data on past and upcoming games...")
    for date, url, state in tqdm(game_urls):
        game_url = 'https://statsapi.mlb.com' + url
        res = requests.get(game_url).text
        game_data = json.loads(res)

        # Get HOME and AWAY team names
        try:
            home = game_data['gameData']['teams']['home']['abbreviation']
            away = game_data['gameData']['teams']['away']['abbreviation']

        except:
            home = game_data['gameData']['teams']['home']['name']['abbrev']
            away = game_data['gameData']['teams']['away']['name']['abbrev']

        home = convert_name(home, how='abbr')
        away = convert_name(away, how='abbr')

        # Store game id as an identifier
        gid = url.split('/')[4]

        # Push to database
        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date,
                         'gid'  : gid},
                        {'$set' : {'state': state,
                                   'preview': []}})

        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date,
                         'gid'  : gid},
                         {'$push': {'preview': game_data}}, upsert=True)

        # Add savant preview data
        savant_preview(gid, date)

        # Add baseballpress lineups
        lineups(date)


def savant_preview(gid, date):
    url = 'https://baseballsavant.mlb.com/preview?game_pk={}&game_date={}'\
                                                        .format(gid, date)
    res = requests.get(url).text
    soup = BeautifulSoup(res, "html.parser")
    jstr = soup.find_all('script')[9].contents[0].strip()\
                                                 .rstrip(';')\
                                                 .lstrip('var teams = ')
    game_data = json.loads(jstr)

    db.Games.update_one({'date' : date,
                         'gid'  : gid},
                         {'$set': {'savant.preview' : []}})

    db.Games.update_one({'date' : date,
                         'gid'  : gid},
                        {'$push': {'savant.preview': game_data}})


def savant_previews(date=None):
    date = datetime.date.today().strftime('%Y-%m-%d') if not date else date
    url= 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}'\
                                                        .format(date)
    res = json.loads(requests.get(url).text)

    gids = [str(x['gamePk']) for x in res['dates'][0]['games']]


    game_url = 'https://baseballsavant.mlb.com/preview?game_pk={}&game_date={}'

    game_urls = [(gid, game_url.format(gid, date)) for gid in gids]

    for gid, url in tqdm(game_urls):
        res = requests.get(url).text
        soup = BeautifulSoup(res, "html.parser")
        jstr = soup.find_all('script')[9].contents[0].strip()\
                                                     .rstrip(';')\
                                                     .lstrip('var teams = ')
        game_data = json.loads(jstr)

        db.Games.update_one({'date' : date,
                             'gid'  : gid},
                            {'$push': {'savant.preview': game_data}})


def espn_preview_text(date, team):
    # Find competition ID
    date_norm = date.replace('-', '')
    url = 'http://www.espn.com/mlb/schedule/_/date/{}'.format(date_norm)
    html = requests.get(url).text.replace('\t', '')

    data = re.search(r'data: [\s\S]+,\nqueue', html, re.DOTALL).group()
    data = data.strip(',\nqueue').strip('data: ')
    j = json.loads(data)

    events = j['sports'][0]['leagues'][0]['events']

    event = [event for event in events if
             team == event['competitors'][0]['abbreviation'] or
             team == event['competitors'][1]['abbreviation']][0]

    cid = event['competitionId']

    # Scrape article text
    url = 'http://www.espn.com/mlb/preview?gameId={}'.format(cid)
    html = requests.get(url).text.replace('\t', '')

    soup = BeautifulSoup(html, 'html.parser')

    mlbid = 'mlbid' + cid

    article = soup.find('article', {'data-id' : '{}'.format(mlbid)})
    text = ' '.join([p.text for p in article.find_all('p')])
    text = text.replace("\'", "'")

    # Find correct game object
    gid = list(dbc.get_team_game_preview(team, date))[0]['gid']

    # Push to database
    db.Games.update({'gid' : gid},
                    {'$set' : {'preview_text' : text}})


def league_elo():
    """
    - Rank
    - Team
    - Rating
    - Playoffs %
    - Division %
    - World Series %
    """
    url = 'https://projects.fivethirtyeight.com/2019-mlb-predictions/'
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
        db_data  = parse_types(db_data)

        # Clear existing elo document
        tm = convert_name(name=team, how='abbr')
        db.Teams.update_many({'Tm' : tm}, {'$set': {'elo' : []}})

        db.Teams.update_many({'Tm' : tm},
                             {'$push': {'elo' : db_data}})


def team_logos():
    url = 'https://www.mlb.com/team'
    soup = open_url(url)

    teams = soup.find_all('div', {'class' : 'p-featured-content'})

    for team in tqdm(teams):

        # Get team abbreviation from url
        team_url = team.find('a', href=True)['href'].lstrip('/help/')

        if 'form' in team_url:
            name = team_url.split('.com/')[-1].split('/')[0]
        elif '=' in team_url:
            name = team_url.split('=')[-1]
        else:
            name = 'nym'
        abbr = convert_name(name)

        # Save image data
        imgs = team.find('div', {'class' : 'p-image'}).img.get('data-srcset')
        url_str = [url for url in imgs.split() if '320x180' in url][0]
        img_url = url_str.split(',')[-1]

        req = requests.get(img_url).content
        img = Image.open(BytesIO(req))

        # db.Teams.update({'Tm' : abbr},
        #                 {'$set': {'logo': img.tobytes()}})

        # Save locally
        path = os.path.join(os.getcwd(), 'logos')
        if not os.path.exists(path):
            os.mkdir(path)

        fn = "{}/{}.png".format(path, abbr)
        img.save(fn)


def fte_prediction(year=None):
    """
    Scrape game predictions from:
    https://projects.fivethirtyeight.com/2018-mlb-predictions/games/
    """
    today = datetime.date.today().strftime('%Y-%m-%d')
    year = today.split('-')[0] if not year else year

    url = 'https://projects.fivethirtyeight.com/{}-mlb-predictions/games/'\
                                                                .format(year)
    soup = open_url(url)

    table = soup.find('table', {'class' : 'table'}).find('tbody')
    rows = table.find_all('tr')

    def extract_date(datestr):
        date = ''
        for char in datestr:
            if not char.isalpha():
                date += char
            else:
                break
        m, d = date.split('/')

        if len(m) < 2:
            m = '0' + m
        if len(d) < 2:
            d = '0'

        datef = '2019-{}-{}'.format(m, d)
        return datef

    # Iterate over rows and account for double headers
    games, game = defaultdict(list), {}
    date, dh = None, False
    for row in rows:
        data = [x.text for x in row.find_all('td')]

        # Home and away teams on separate row. Only one row has the date.
        if not date:
            date, team, win = extract_date(data[0]), data[1][:3], data[7]
            game[team] = win
        else:
            team, win = data[0][:3], data[6]
            game.update({team : win})

            games[date].append(game)
            date = None
            game = {}

    # Add win % to db for each game today
    seen = []
    for game in games[today]:
        home, away = list(game.keys())

        g = Game()
        todays_game = list(g.get_team_game_preview(home, today))

        if not todays_game:
            continue

        # Double headers will have two games in todays_game array
        if len(todays_game) > 1:
            earlier_idx = g.compare_game_times(todays_game)
            later_idx = 1 if earlier_idx == 0 else 0

            if home in seen:
                gid = todays_game[later_idx]['gid']
            else:
                gid = todays_game[earlier_idx]['gid']
        else:
            gid = todays_game[0]['gid']

        g._db.Games.update({'gid'  : gid}, {'$set':  {'fte' : []}})
        g._db.Games.update({'gid' : gid}, {'$push': {'fte' : game}})

        # Add home team to seen list to catch double headers
        seen.append(home)


def lineups(date=None):
    date = datetime.date.today().strftime('%Y-%m-%d') if not date else date
    url = 'https://www.baseballpress.com/lineups/{}'.format(date)
    res = requests.get(url).text
    soup = BeautifulSoup(res, 'html.parser')
    cards = soup.find_all('div', {'class' : 'lineup-card'})
    for card in cards:
        lineup = [x for x in card.find('div', {'class' : 'lineup-card-body'})\
                                      .find_all('div', {'class' : 'player'})]
        ldata = []
        for player in lineup:
            pstats = {}
            pstats.update({'order' : player.text.split('.')[0]})
            pstats.update({'pid' : player.find('a',
                                  {'class' : 'player-link'})['data-mlb']})
            try:
                pstats.update({'name' : player.find('span').text})
            except:
                pstats.update({'name' : player.find('a').text})

            ldata.append(pstats)

        away, home  = [x['href'].split('/')[-1] for x in
                       card.find('div', {'class' : 'lineup-card-header'})\
                                                          .find_all('a')][:2]

        db_data = {}
        db_data.update({convert_name(away) : ldata[:9]})
        db_data.update({convert_name(home) : ldata[9:]})

        db.Games.update_one({'date' : date,
                             'home' : convert_name(home),
                             'away' : convert_name(away)},
                            {'$set': {'baseballpress.lineup' : []}})

        db.Games.update_one({'date' : date,
                             'home' : convert_name(home),
                             'away' : convert_name(away)},
                            {'$push': {'baseballpress.lineup': db_data}})


if __name__ == '__main__':
    year = datetime.date.today().strftime('%Y')

    game_previews()
    print("Scraping past boxscores...")
    boxscores()
    fte_prediction()
    team_logos()

    print("Scraping batter and pitcher leaderboards")
    fangraphs('bat', year)
    fangraphs('pit', year)

    fangraphs_splits(year=year)

    print("Scraping league elo and division standings")
    standings()

    print("Scraping schedule, roster, pitch logs, injuries, transactions...")
    teams = ['laa', 'hou', 'oak', 'tor', 'atl', 'mil',
             'stl', 'chc', 'ari', 'lad', 'sfg', 'cle',
             'sea', 'mia', 'nym', 'wsn', 'bal', 'sdp',
             'phi', 'pit', 'tex', 'tbr', 'bos', 'cin',
             'col', 'kcr', 'det', 'min', 'chw', 'nyy']
    for team in tqdm(teams):
        schedule(team)
        pitching_logs(team, year)
        current_injuries(team)
        transactions(team, year)
        forty_man(team, year)

    league_elo()
