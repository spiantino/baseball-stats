from bs4 import BeautifulSoup, Comment
from tqdm import tqdm
import requests
import argparse
import re
import json
import datetime
import pickle
import inspect
import pandas as pd
from io import StringIO

from dbcontroller import DBController
from utils import open_url, convert_name, find_missing_dates, parse_types

# No push methods in DBController, so do it manually for now
dbc = DBController()
db = dbc._db


def fangraphs(state, year):
    """
    Scrape data from fangraphs.com
    """
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

        # Convert team name to abbreviation
        try:
            db_data['Team'] = convert_name(db_data['Team'])
        except:
            pass # any need to pull team value from br here?
            # print("(fangraphs) No team listed for {}".format(player))

        # Store type as numeric if possible
        db_data = parse_types(db_data)

        # Insert row into database
        db_path = 'fg.{}.{}'.format(state, year)
        db.Players.update({'Name': player},
                          {'$set' : {db_path : db_data}}, upsert=True)

        # Add current team to top level
        if year == dbc._year:
            db.Players.update({'Name' : player},
                              {'$set': {'Team' : db_data['Team']}})

def fangraph_splits(year):
    # 5 is for left handed batters, 6 for right handed batters
    for hand in [5, 6]:
        url = """https://www.fangraphs.com/leaderssplits.aspx?splitArr={0}\
               &strgroup=season&statgroup=1&startDate={1}-03-01\
               &endDate={1}-11-01&filter=&position=P&statType=player\
               &autoPt=true&players=&sort=19,-1&pg=0"""\
               .format(hand, year).replace(' ', '')

        soup = open_url(url)

        # Send POST request to get data in csv format
        params = {'__EVENTTARGET' : 'SplitsLeaderboard$cmdCSV',
                  '__EVENTARGUMENT' : '',
                  'SplitsLeaderboard$dataPlayerId': 'all',
                  'SplitsLeaderboard$dataPos' : 'P',
                  'SplitsLeaderboard$dataSplitArr': '[{}]'.format(hand),
                  'SplitsLeaderboard$dataGroup' : 'season',
                  'SplitsLeaderboard$dataType' : '1',
                  'SplitsLeaderboard$dataStart' : '{}-03-01'.format(year),
                  'SplitsLeaderboard$dataEnd' : '{}-11-01'.format(year),
                  'SplitsLeaderboard$dataSplitTeams' : 'false',
                  'SplitsLeaderboard$dataFilter' : '[]',
                  'SplitsLeaderboard$dataAutoPt' : 'true',
                  'SplitsLeaderboard$dataStatType' : 'player',
                  'SplitsLeaderboard$dataPlayers' : ''}

        elems = ['__VIEWSTATE',
                 '__VIEWSTATEGENERATOR',
                 '__EVENTVALIDATION']

        # Find dynamic parameters in the page html
        more_params = [soup.find('input', {'id' : elem}) for elem in elems]
        for param in more_params:
            params.update({param['id'] : param['value']})

        req = requests.post(url, data=params).text
        df = pd.read_csv(StringIO(req))

        # Push one row at a time into database
        df_data = df.to_dict(orient='index')
        for key in tqdm(df_data.keys()):
            name = df_data[key]['Name']
            season = df_data[key]['Season']
            player_data = {k:v for k,v in df_data[key].items()
                               if k not in ['Name', 'Season']}

            handstr = 'vLHH' if hand == 5 else 'vRHH'
            db_path = 'fg.{}.{}'.format(handstr, season)

            db.Players.update({'Name' : name},
                              {'$set' : {db_path : player_data}})


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
    cols = [x.text.replace('\xa0', 'Field').replace('.', '') for x in thead.find_all('th')]
    upcoming_cols = cols[:6] + ['Time']

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

        db_data = parse_types(db_data)

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
        db_data = parse_types(db_data)

        # Insert row indo database
        db.Teams.update({'Tm' : team},
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
    db.Teams.update({'Tm' : team},
                    {'$set': {db_array : []}})

    # Extract forty-man roster and push to database
    for row in tqdm(trows):
        bid = row.find('a')['href'].split('=')[-1]
        row_data = [x.text for x in
                    row.find_all(lambda tag: tag.has_attr('data-stat'))]
        db_data = {k:v for k,v in zip(cols, row_data)}
        db_data.update({'bid' : bid})
        db.Teams.update({'Tm' : team},
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
    pit_or_bat = table.find('caption').text

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
            db.Players.update({'Name' : name},
                              {'$set' : {'brID' : brid,
                                         db_array : db_data}}, upsert=True)

    # Extract Player Value Table - Stored in html comment
    comment = soup.find_all(string=lambda text: isinstance(text, Comment))
    comment_html = [x for x in comment if 'Player Value' in x]

    for c in comment_html:
        table = BeautifulSoup(c.string, "html.parser")

        table_name = table.find('caption').text.replace('--', ' ').split()
        title = '{} {}'.format(table_name[2], table_name[1])

        thead = table.find('thead')
        cols = [x.text for x in thead.find_all('th')]

        tbody = table.find('tbody')
        trows = tbody.find_all('tr')

        for row in trows:
            row_data = [x.text for x in
                        row.find_all(lambda tag: tag.has_attr('data-stat'))]
            db_data = {k:v for k,v in zip(cols, row_data)}
            db_data = parse_types(db_data)

            # Rename stats to match fg data
            renames = {'BA' : 'AVG'}
            for stat in rename.keys():
                db_data[renames[stat]] = db_data[stat]
                db_data.pop(stat)

            # Skip blank rows and don't collect minor league data
            if not row_data[0] or db_data['Lg'] not in ['AL', 'NL']:
                continue

            db_array = 'br.{}.{}'.format(title, db_data['Year'])
            db.Players.update({'brID' : brid},
                              {'$set' : {db_array : db_data}}, upsert=True)


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
        db_data = parse_types(db_data)
        db.Teams.update({'Tm' : team},
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

    # Name transactions array by year in database
    db_array = 'Transactions.{}'.format(year)

    # Clear existing Transactions document
    db.Teams.update({'Tm' : team.upper()},
                    {'$set': {db_array : []}})

    # Add Transactions json data to database
    db.Teams.update({'Tm' : team.upper()},
                    {'$push' : {db_array : j}})


def boxscores(date, dbc=dbc):
    """
    Extract all boxscores
    """

    # Delete games with postponed status to avoid conflict
    dbc.delete_duplicate_game_docs()

    year = datetime.date.today().strftime('%Y')

    if date == 'all':
        url = 'https://www.baseball-reference.com/leagues/MLB/{}-schedule.shtml'.format(year)

        soup = open_url(url)

        # Find links for each game this season
        all_game_urls = [x['href'] for x in soup.find_all('a', href=True)
                                         if x.text=='Boxscore']

        dates = dbc.get_missing_array_dates('summary')

        # Format dates to match date in url
        datesf = [x.replace('-', '') for x in dates]

        # Filter games by missing dates
        game_urls = [game for game in all_game_urls
                                   if any(date in game for date in datesf)]
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
        gid = games[idx]['gid']

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


def game_previews(dbc=dbc):
    """
    Collect data on upcomming game
    from mlb.com/gameday
    """
    dates = set(find_missing_dates(dbc=dbc)).union({'2018-06-29'})
    outdated = set(dbc.find_outdated_game_dates())

    dates = dates.union(outdated)
    dates = sorted(list(dates))

    url_string = 'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={}'

    urls = [(date, url_string.format(date)) for date in dates]

    print("Gathering game data urls...")
    game_urls = []
    for date, url in tqdm(urls):
        res = requests.get(url).text
        schedule_data = json.loads(res)

        games_data = schedule_data['dates'][0]['games']

        # Gather all game urls
        gdata = [(date, game['link'], game['status']['detailedState'])
                                              for game in games_data]
        # Only collect data on scheduled games (not postponed or other)
        valid_states = ['Scheduled',
                        'Pre-Game',
                        'Warmup',
                        'In Progress',
                        'Final',
                        'Game Over']
        valid_games = [game for game in gdata if game[2] in valid_states]
        game_urls += valid_games

        # Remove postponed game docs from database
        all_urls   = [g[1] for g in gdata]
        valid_urls = [g[1] for g in valid_games]
        invalid_urls = list(set(all_urls) - set(valid_urls))
        invalid_gids  = [url.split('/')[4] for url in invalid_urls]
        invalid_games = dbc.query_by_gids(invalid_gids)
        if invalid_games.count():
            dbc.remove_games(invalid_gids)

    # Collect data on all upcoming games
    print("Collecting game data on past and upcoming games...")
    for date, url, state in tqdm(game_urls):
        game_url = 'https://statsapi.mlb.com' + url
        res = requests.get(game_url).text
        game_data = json.loads(res)

        # Get HOME and AWAY team names
        if state == 'Scheduled':
            home = game_data['gameData']['teams']['home']['abbreviation']
            away = game_data['gameData']['teams']['away']['abbreviation']
        else:
            home = game_data['gameData']['teams']['home']['name']['abbrev']
            away = game_data['gameData']['teams']['away']['name']['abbrev']

        # Change game state to match state on the preview page
        if state == 'Game Over':
            state = 'Final'
        game_data['gameData']['status']['detailedState'] = state

        home = convert_name(home, how='abbr')
        away = convert_name(away, how='abbr')

        # Store game id as an identifier
        gid = url.split('/')[4]

        # Push to database
        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date,
                         'gid'  : gid},
                        {'$set' : {'preview': []}})

        db.Games.update({'home' : home,
                         'away' : away,
                         'date' : date,
                         'gid'  : gid},
                         {'$push': {'preview': game_data}}, upsert=True)


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
        db_data  = parse_types(db_data)

        # Clear existing elo document
        tm = convert_name(name=team, how='abbr')
        db.Teams.update({'Tm' : tm}, {'$set': {'elo' : []}})

        db.Teams.update({'Tm' : tm},
                        {'$push': {'elo' : db_data}})


if __name__ == '__main__':
    year = datetime.date.today().strftime('%Y')

    # fangraph_splits(year=year)

    print("Scraping past boxscores...")
    boxscores(date='all')

    print("Scraping batter and pitcher leaderboards")
    fangraphs('bat', year)
    fangraphs('pit', year)

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
