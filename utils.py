from bs4 import BeautifulSoup
import requests
import datetime
from collections import defaultdict
import pandas as pd
import re

def open_url(url):
    page = requests.get(url)
    return BeautifulSoup(page.text, "html.parser")


def convert_name(name, how='abbr'):
    """
    Convert between abbreviation and full team name.
    Also resolves abbreviation differences.
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

    abbrfix = {
                'kc'  : 'kcr',
                'sd'  : 'sdp',
                'sf'  : 'sfg',
                'tb'  : 'tbr',
                'la'  : 'lad',
                'kc'  : 'kcr',
                'ana' : 'laa',
                'cws' : 'chw',
                'was' : 'wsn',
                'wsh' : 'wsn',
                'nya' : 'nyy'
                }

    name = name.lower()

    if name == 'all':
        return name

    elif how == 'full':
        if len(name) == 3:
            return abbr2full[name].upper()
        else:
            return name

    elif how == 'abbr':
        if len(name) != 2 and len(name) != 3:
            return full2abbr[name].upper()
        try:
            return abbrfix[name].upper()
        except:
            return name.upper()


def get_stadium_location(team):
    team = convert_name(team)
    locs = {
            'ARI': {'lat': 33.4455, 'lon': -112.0667},
            'ATL': {'lat': 33.7345, 'lon': -84.3870},
            'BAL': {'lat': 39.2840, 'lon': -76.6199},
            'BOS': {'lat': 42.3467, 'lon': -71.0972},
            'CHC': {'lat': 41.8299, 'lon': -87.6338},
            'CHW': {'lat': 41.8299, 'lon': -87.6338},
            'CIN': {'lat': 39.0979, 'lon': -84.5082},
            'CLE': {'lat': 41.4962, 'lon': -81.6852},
            'COL': {'lat': 39.7559, 'lon': -104.9942},
            'DET': {'lat': 42.3400, 'lon': -83.0456},
            'HOU': {'lat': 29.7573, 'lon': -95.3555},
            'KCR': {'lat': 39.0517, 'lon': -94.4803},
            'LAA': {'lat': 33.8003, 'lon': -117.8827},
            'LAD': {'lat': 34.0739, 'lon': -118.2400},
            'MIA': {'lat': 25.7783, 'lon': -80.2196},
            'MIL': {'lat': 43.0280, 'lon': -87.9712},
            'MIN': {'lat': 44.9757, 'lon': -93.2737},
            'NYM': {'lat': 40.7571, 'lon': -73.8458},
            'NYY': {'lat': 40.8296, 'lon': -73.9262},
            'OAK': {'lat': 37.7516, 'lon': -122.2005},
            'PHI': {'lat': 39.9061, 'lon': -75.1665},
            'PIT': {'lat': 40.4469, 'lon': -80.0057},
            'SDP': {'lat': 32.7077, 'lon': -117.1569},
            'SEA': {'lat': 47.5952, 'lon': -122.3316},
            'SFG': {'lat': 37.7786, 'lon': -122.3893},
            'STL': {'lat': 38.6226, 'lon': -90.1928},
            'TBR': {'lat': 27.7682, 'lon': -82.6534},
            'TEX': {'lat': 32.7512, 'lon': -97.0832},
            'TOR': {'lat': 43.6414, 'lon': -79.3894},
            'WSN': {'lat': 38.8730, 'lon': -77.0074}
            }
    return locs[team]


# def find_missing_dates(dbc):
#     """
#     Find dates between start of season and yesterday
#     where no Games docs have been recorded for that date
#     !!! return all_dates and do set operations elsewhere,
#     so that dbc doesn't need to be used here
#     """
#     dates = dbc.get_past_game_dates()

#     url = 'https://www.baseball-reference.com/leagues/MLB/2018-schedule.shtml'
#     soup = open_url(url)

#     # Find and format season start date
#     start = soup.find('div', {'class' : 'section_content'}).find('h3').text
#     start = ' '.join(start.split()[1:4])
#     start = str(datetime.datetime.strptime(start, '%B %d, %Y').date())

#     # yesterday = ((datetime.date.today() -
#     #               datetime.timedelta(1))
#     #                       .strftime('%Y-%m-%d'))
#     today = datetime.date.today()
#     date_range = pd.date_range(start=start, end=today)
#     all_dates = set([str(x.date()) for x in date_range])

#     return list(sorted(all_dates - dates))


def combine_dicts_in_list(list_of_dicts):
    """
    Convert db format into df format
    """
    data = defaultdict(list)
    for d in list_of_dicts:
        for k, v in d.items():
            data[k].append(v)
    return data


def find_earlier_date(list_with_dates):
    """
    Return object id that contains the earlier date
    Input list contains dicts with '_id' and 'date' keys
    """
    id1 = list_with_dates[0]['_id']
    id2 = list_with_dates[1]['_id']

    date1 = list_with_dates[0]['date']
    date2 = list_with_dates[1]['date']

    date1 = datetime.datetime.strptime(date1, '%Y-%m-%d')
    date2 = datetime.datetime.strptime(date2, '%Y-%m-%d')

    return id1 if date1 < date2 else id2


def subtract_dates(date1, date2):
    """
    Return number of days between two dates
    Input dates are in fomrat 'YYYY-mm-dd'
    """
    date1 = datetime.datetime.strptime(date1, '%Y-%m-%d')
    date2 = datetime.datetime.strptime(date2, '%Y-%m-%d')
    days = (date1-date2).days
    return str(days).strip('-')

def get_last_week_dates():
    """
    Find dates from last week and return in a sorted list
    """
    today = datetime.date.today()
    weekday = today.weekday()

    start = datetime.timedelta(days=weekday, weeks=1)
    start_of_week = today - start
    end_of_week = start_of_week + datetime.timedelta(6)

    date_range = pd.date_range(start_of_week, end_of_week).astype(str)
    return list(date_range)[::-1]

def parse_types(d):
    """
    Parse values in db_data dict and
    convert to int or float when possible
    """
    new_dict = {}
    for k,v in d.items():
        if v and all([char.isdigit() for char in str(v)]):
            new_dict[k] = int(v)
        else:
            try:
                new_dict[k] = float(v)
            except:
                new_dict[k] = v
    return new_dict

def normalize_name(name):
    pattern = 'Jr.|Sr.|II|III|'
    return re.sub(pattern, '', name).strip()
