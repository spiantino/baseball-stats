from bs4 import BeautifulSoup
import requests
import datetime
from collections import defaultdict
import pandas as pd

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
                'cws' : 'chw',
                'was' : 'wsn',
                'wsh' : 'wsn'
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


def find_missing_dates(dbc):
    """
    Find dates between start of season and yesterday
    where no Games docs have been recorded for that date
    !!! return all_dates and do set operations elsewhere,
    so that dbc doesn't need to be used here
    """
    dates = dbc.get_past_game_dates()

    url = 'https://www.baseball-reference.com/leagues/MLB/2018-schedule.shtml'
    soup = open_url(url)

    # Find and format season start date
    start = soup.find('div', {'class' : 'section_content'}).find('h3').text
    start = ' '.join(start.split()[1:4])
    start = str(datetime.datetime.strptime(start, '%B %d, %Y').date())

    # yesterday = ((datetime.date.today() -
    #               datetime.timedelta(1))
    #                       .strftime('%Y-%m-%d'))
    today = datetime.date.today()
    date_range = pd.date_range(start=start, end=today)
    all_dates = set([str(x.date()) for x in date_range])

    return list(sorted(all_dates - dates))


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

