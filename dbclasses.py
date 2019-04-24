from datetime import datetime, time
import math
import pytz
from  dateutil.parser import parse
from collections import Counter, defaultdict
from fractions import Fraction
from unidecode import unidecode
from PIL import Image

from dbcontroller import DBController
from utils import convert_name, access_array

class Player(DBController):
    def __init__(self, test=True, name=None):
        self._name = name
        super().__init__(test)

    def by_name(self, name):
        self._name = name
        # return self._db.Players.find_one({'Name' : name})

    def update_stat(self, site, year, pos, stat, value):
        stat_path = '{}.{}.{}.{}'.format(site, year, pos, stat)
        self._db.Players.update({'Name' : self._name},
                                {'$set': {stat_path : value}})

    def get_stat(self, stat, pos='bat', site='fg', year=None):
        year = self._year if not year else year
        stat_path = '${}.{}.{}.{}'.format(site, pos, year, stat)
        res = self._db.Players.aggregate([{'$match': {'Name' : self._name}},
                                          {'$project': {'_id' : 0,
                                                        'stat' : stat_path}}])
        try:
            return list(res)[0]['stat']
        except:
            return None

    def get_stats(self, stats, pos='bat', site='fg', year=None):
        year = self._year if not year else year
        stat_path = {stat : '${}.{}.{}.{}'.format(site, pos, year, stat)
                                                       for stat in stats}
        stat_path.update({'_id' : 0})
        res = self._db.Players.aggregate([{'$match': {'Name' : self._name}},
                                          {'$project': stat_path}])
        try:
            stat_array = list(res)[0]
        except:
            missing_player = {'Name' : self._name}
            missing_player.update({stat: None for stat in stats})
            return missing_player

        # Look for missing stats in br data
        for stat in stats:
            if stat not in stat_array.keys():
                val = self.get_stat(site='br',
                                    pos=pos,
                                    stat=stat)

                stat_array[stat] = val

        return stat_array

    def get_starters_or_relievers(self, role, pos, year):
        """
        Return list of starter or reliver object ids
        """
        cond    = '$lt' if role=='reliever' else '$gt'
        gp      = 'fg.{}.{}.G'.format(pos, year)
        war_val = '$fg.{}.{}.WAR'.format(pos, year)
        gp_val  = '$fg.{}.{}.G'.format(pos, year)
        gs_val  = '$fg.{}.{}.GS'.format(pos, year)

        objs = self._db.Players.aggregate([{'$match' : {gp: {'$gt':  0}}},
                                           {'$project':
                                               {'GS%' :
                                                    {'$divide': [gs_val,
                                                                 gp_val]}}},
                                            {'$match':
                                                {'GS%': {cond: 0.5}}}
                                            ])
        return [x['_id'] for x in objs]

    def get_top_n_pitchers(self, role, year, stat, ascending, n):
        """
        Return top n pitchers sorted by stat
        """
        sort_key = 'fg.pit.{}.{}'.format(year, stat)
        ids = self.get_starters_or_relievers(role=role, year=year, pos='pit')
        sort_direction = 1 if ascending else -1

        if stat == 'ERA':
            pit_data = '$fg.pit.{}'.format(year)
            ip = '$fg.pit.{}.IP'.format(year)
            res = self._db.Players.aggregate([
                                {'$match': {'_id': {'$in' : ids}}},
                                {'$lookup': {'from': 'Teams',
                                             'localField': 'Team',
                                             'foreignField': 'Tm',
                                             'as': 'td'}},
                                {'$unwind': '$td'},
                                {'$project': {'_id': 0,
                                              'pit' : pit_data,
                                              'qual': {'$subtract':
                                                            [ip, '$td.G']}}},
                                {'$match': {'qual': {'$gte': 0}}},
                                {'$project': {'pit': '$pit'}},
                                {'$sort': {'pit.ERA': 1}}])
            return [x['pit'] for x in list(res)[:n]]

        else:
            res = self._db.Players.find({'_id' : {'$in' : ids}})\
                                  .sort(sort_key, sort_direction).limit(n)
            return [x['fg']['pit'][str(year)] for x in res]


    def get_top_n_leaders(self, pos, stat, year, n):
        """
        Query top 10 batting or pitching leaderboard
        """
        sort_key = 'fg.{}.{}.{}'.format(pos, year, stat)

        lb = self._db.Players.find({}).sort(sort_key, -1).limit(n)

        return [x['fg'][pos][str(year)] for x in lb]

    def get_top_n_homerun_leaders(self, year, n):
        sort_key = '{}.bat.HR'.format(year)
        return self._db.Players.find({}).sort(sort_key, -1)


class Game(DBController):
    def __init__(self, test=True):
        self._game = None
        super().__init__(test)

    def get_game_preview(self):
        return self._preview

    def query_game_preview_by_date(self, team, date, idx=None):

        games = self._db.Games.find({'date' : date,
                                     '$or' : [{'home' : team},
                                              {'away' : team}]})

        if idx != None:
            game = list(games)[idx]
        else:
            game = self.extract_game(games)

        if not game:
            print("Error: No game found for {}".format(date))

        else:
            self._date = date
            self._team = team
            self._side = 'home' if game['home'] == team else 'away'
            self._opp_side = 'home' if self._side == 'away' else 'away'
            self._opp = game[self._opp_side]
            self._game = game
            self._preview = game['preview'][0]
            self._savant = game['savant']['preview'][0]
            self._state = self._preview['gameData']['status']['detailedState']
            self._players = list(self._preview['gameData']['players'].values())

    def extract_game(self, cursor):
        """
        Return game data from a cursor object
        Determine which game to return when a double header occurs
        """
        games = list(cursor)
        if len(games) == 2:
            status = [game['preview'][0]['gameData']
                          ['status']['detailedState']
                                   for game in games]

            # Return later game if earlier game has already started or
            target_states = ['Final', 'Game Over', 'In Progress']
            if any([state in target_states for state in status]):
                idx = [status.index(x) for x in status if x!='Final'][0]
                return games[idx]

            # Find and return earlier game
            else:
                idx = self.compare_game_times(games)
                return games[idx]

        elif len(games) == 1:
            return games[0]

        else:
            return None

    def compare_game_times(self, games):
        """
        Return list index of game with the earlier start time
        """
        am_or_pm = [game['preview'][0]['gameData']['datetime']['ampm']
                                                    for game in games]
        if any([time == 'AM' for time in am_or_pm]):
            idx = am_or_pm.index('AM')
            return idx

        else:
            times = [game['preview'][0]['gameData']['datetime']['time']
                                                     for game in games]
            time_ints = [int(time.replace(':', '')) for time in times]
            earlier_time = min(time_ints)
            return time_ints.index(earlier_time)

    def get_team_game_preview(self, team, date):
        """
        Query game preview for specific team
        """
        abbr = convert_name(team, how='abbr')
        return self._db.Games.find({'date' : date,
                                    '$or' : [{'home' : abbr},
                                             {'away' : abbr}]})

    def parse_game_details(self):
        game_data = self._preview['gameData']
        live_data = self._preview['liveData']

        home = game_data['teams']['home']
        away = game_data['teams']['away']

        home_name = home['name']
        away_name = away['name']
        home_abbr = convert_name(home['abbreviation'])
        away_abbr = convert_name(away['abbreviation'])
        home_rec  = home['record']['leagueRecord']
        away_rec  = away['record']['leagueRecord']

        raw_game_time = game_data['datetime']['time']
        hour = int(raw_game_time.split(':')[0]) + 1
        mins = raw_game_time.split(':')[1]
        game_time = "{}:{}".format(hour, mins)

        wind_dir = ''

        home_score = None
        away_score = None

        home_wins = home_rec['wins']
        away_wins = away_rec['wins']

        home_losses = home_rec['losses']
        away_losses = away_rec['losses']

        am_or_pm = game_data['datetime']['ampm']
        stadium = game_data['venue']['name']

        # Five Thirty Eight prediction
        # If past games do not have this data, just assign None
        try:
            preds = self._game['fte'][0]
            if self._side == 'home':
                home_pred = preds[self._team]
                away_pred = preds[self._opp]
            else:
                home_pred = preds[self._opp]
                away_pred = preds[self._team]
        except:
            home_pred = None
            away_pred = None


        self._game_details = {'homeName' : home_name,
                              'homeAbbr' : home_abbr,
                              'homeWins' : home_wins,
                              'homeLoss' : home_losses,
                              'homeScore': home_score,
                              'awayName' : away_name,
                              'awayAbbr' : away_abbr,
                              'awayWins' : away_wins,
                              'awayLoss' : away_losses,
                              'awayScore': away_score,
                              'homePred' : home_pred,
                              'awayPred' : away_pred,
                              'gameTime' : game_time,
                              'windDir'  : wind_dir,
                              'am_or_pm' : am_or_pm,
                              'stadium'  : stadium }

    def parse_starting_pitchers(self):

        def extract_data(side):
            pitchers = self._preview['gameData']['probablePitchers']
            players = self._preview['gameData']['players']

            pid  = 'ID' + str(pitchers[side]['id'])
            data = players[pid]
            name = access_array(data, 'fullName')
            num  = access_array(data, 'primaryNumber')
            hand = access_array(data, 'pitchHand', 'code')

            team = self._game[side]

            return {'pid'  : pid,
                    'name' : name,
                    'num'  : num,
                    'hand' : hand,
                    'team' : team}

        home_pitcher = extract_data('home')
        away_pitcher = extract_data('away')

        self._pitchers = {'home': home_pitcher,
                          'away': away_pitcher}

    def parse_pitcher_game_stats(self):
        """
        Return pitching stats for the current game
        """
        self._pitcher_gamestats = {}
        for side in ['home', 'away']:
            name = self._pitchers[side]['name']
            pid  = self._pitchers[side]['pid']
            data = self._preview['liveData']['boxscore']['teams'][side]\
                                ['players'][pid]['gameStats']['pitching']

            stats = {'name' : name,
                     'ipit' : access_array(data, 'inningsPitched'),
                     'hrun' : access_array(data, 'homeRuns'),
                     'erun' : access_array(data, 'earnedRuns'),
                     'hits' : access_array(data, 'hits'),
                     'runs' : access_array(data, 'runs'),
                     'walk' : access_array(data, 'baseOnBalls'),
                     'strk' : access_array(data, 'strikeOuts')}

            # Find GSc from BR boxscore data
            decoded = unidecode(name)
            team = self._pitchers[side]['team']

            br_data = self._game[team]['pitching']
            pdata = [data for data in br_data
                           if data['Pitching'] == decoded]

            gsc = access_array(pdata, 0, 'GSc')

            if not gsc:
                print("BR Boxscores missing for {} - {}".format(team,
                                                                self._date))

            stats.update({'gsc' : gsc})

            self._pitcher_gamestats[side] = stats


    def parse_br_pitching_data(self):
        """
        Parse pitching stats from BR boxscores:
        - WPA, Pitches, IP, Inning entered

        Add data to get same results as parse_pitcher_game_stats (depricated)
        """
        self._br_pit_data = {'home' : {}, 'away' : {}}

        for side in ['home', 'away']:

            team = self._game[side]
            pit_data = self._game[team]['pitching'][1:]
            inning_idx = 0

            for data in pit_data:
                name = data['Pitching']
                ip = float(data['IP'])

                # Infer inning that pitcher entered the game
                inning_idx += math.floor(ip)
                remainder = round((ip % 1) * 10)
                if remainder > 0:
                    inning_idx += Fraction(remainder, 3)
                entered = math.floor((inning_idx - ip) + 1)

                pdata = {'wpa'  : data['WPA'],
                         'pit'  : data['Pit'],
                         'ip'   : data['IP'],
                         'gsc'  : data['GSc'],
                         'ipit' : data['IP'],
                         'hrun' : data['HR'],
                         'erun' : data['ER'],
                         'hits' : data['H'],
                         'runs' : data['R'],
                         'walk' : data['BB'],
                         'strk' : data['Str'],
                         'name' : data['Pitching'],
                         'entered'  : entered}

                self._br_pit_data[side][name] = pdata

    def parse_bullpen(self):
        live_data = self._preview['liveData']['boxscore']

        def extract_data(side):
            bullpen_data = live_data['teams'][side]['bullpen']
            bullpen_ids = ['ID{}'.format(x) for x in bullpen_data]
            bullpen = {}
            for playerid in bullpen_ids:
                pitcher = live_data['teams'][side]['players'][playerid]
                pit_stats = pitcher['seasonStats']['pitching']

                name = ' '.join((pitcher['name']['first'],
                                 pitcher['name']['last']))

                num = access_array(pitcher, 'shirtNum')

                hits = access_array(pit_stats, 'hits')
                runs = access_array(pit_stats, 'runs')
                eruns = access_array(pit_stats, 'earnedRuns')
                strikeouts = access_array(pit_stats, 'strikeOuts')

                pitcher_data = {'number' : num,
                                'hits' : hits,
                                'earnedRuns' : eruns,
                                'strikeouts' : strikeouts}

                bullpen.update({name : pitcher_data})
            return bullpen

        home_bullpen = extract_data('home')
        away_bullpen = extract_data('away')

        self._bullpen = {'home' : home_bullpen,
                         'away' : away_bullpen}

    def parse_savant_batters(self):

        def extract_data(side):
            roster = self._savant[side]['roster']['hitters']
            starters = []
            bench = []
            for b in roster:
                batter = {
                      'Name'  : access_array(b, 'person', 'fullName'),
                      'Pos'   : access_array(b, 'position', 'name'),
                      'Number': access_array(b, 'jerseyNumber')
                      }

                if access_array(b, 'gameStatus', 'isOnBench'):
                    bench.append(batter)
                else:
                    batter.update({'Order' : access_array(b, 'battingOrder')\
                                                         .replace('0', '')})
                    starters.append(batter)
            return starters, bench

        home_batters, home_bench = extract_data('home')
        away_batters, away_bench = extract_data('away')
        self._savant_batters = {'away_batters' : away_batters,
                                'home_batters' : home_batters,
                                'away_bench'   : away_bench,
                                'home_bench'   : home_bench}

    def parse_bbpress_batters(self):
        lineups = self._game['baseballpress']['lineup'][0]
        self._bbpress_batters = {'away_batters' : lineups[self._game['away']],
                                 'home_batters' : lineups[self._game['home']]}

    def parse_savant_pitchers(self):
        apits = self._savant['away']['roster']['pitchers']
        hpits = self._savant['home']['roster']['pitchers']

        for pit in apits:
            pit['person'].update({'number':pit['jerseyNumber']})

        for pit in hpits:
            pit['person'].update({'number' : pit['jerseyNumber']})

        astarter = self._pitchers['away']['pid'].strip('ID')
        hstarter = self._pitchers['home']['pid'].strip('ID')

        apits = [pit['person']for pit in apits
                                      if pit['person']['id'] != astarter]

        hpits = [pit['person'] for pit in hpits
                                if pit['person']['id'] != hstarter]

        self._all_pitchers = {'away' : apits , 'home' : hpits}

    def parse_pitch_types(self):
        player_data = self._preview['liveData']['players']['allPlayers']
        all_plays = self._preview['liveData']['plays']['allPlays']
        pit_data = defaultdict(list)

        for play in all_plays:
            pid = 'ID' + play['matchup']['pitcher']
            player = ' '.join((player_data[pid]['name']['first'],
                               player_data[pid]['name']['last']))
            decoded = unidecode(player)

            for event in play['playEvents']:
                try:
                    pitch = event['details']['type']
                    pit_data[decoded].append(pitch)
                except:
                    continue
        self._pit_counts = {k : Counter(v) for k,v in pit_data.items()}

    def find_pitch_dates(self, pitcher, team, n=99):
        path = '{}.pitching.Pitching'.format(team)
        res = list(self._db.Games.aggregate([{'$match': {path : pitcher}},
                                             {'$sort': {'date' : -1}},
                                             {'$project': {'date' : '$date'}},
                                             {'$limit': n}]))
        if res:
            return [x['date'] for x in res]
        else:
            return None

    def get_all_start_times(self, date=None):
        today = datetime.today()
        todayf = today.strftime('%Y-%m-%d')
        date = todayf if not date else date
        dtime = '$preview.gameData.datetime.dateTime'
        gtime = '$preview.gameData.datetime.time'
        ampm  = '$preview.gameData.datetime.ampm'

        res = list(self._db.Games.aggregate([{'$match' : {'date': date}},
                                             {'$project': {'_id' : 0,
                                                           'away'  : '$away',
                                                           'home'  : '$home',
                                                           'dtime' : dtime,
                                                           'gtime' : gtime,
                                                           'ampm'  : ampm}}
                                            ]))
        times = {}
        for game in res:
            home, away, dtime, gtime, ampm = game.values()

            if not dtime and not gtime:
                print("No start time listed for {} vs {}".format(away, home))
                continue

            else:
                timef = gtime[0] + ' ' + ampm[0] if not dtime else dtime[0]
                tz = pytz.timezone("America/New_York")
                parsed_time = parse(timef).astimezone(tz)

            # Check for double header before updating times dict
            if home and away in times.keys():
                home2 = home + '_2'
                away2 = away + '_2'

                times[home2] = parsed_time
                times[away2] = parsed_time

            else:
                times[home] = parsed_time
                times[away] = parsed_time

        return times

    def todays_games_in_db(self):
        in_db = list(self._db.Games.find({'date' : self._day}))
        return bool(in_db)

    def parse_all(self):
        if self._game:
            self.parse_game_details()
            self.parse_starting_pitchers()
            self.parse_savant_pitchers()
            self.parse_savant_batters()
            try:    # remove this when database contains full data
                self.parse_bbpress_batters()
            except:
                pass
            self.parse_bullpen()

            if self._state in ["Final", "Game Over", "Completed Early"]:
            #     self.parse_pitcher_game_stats()
            #     self.parse_pitch_types()

                try: # Change this to check if game date != today
                    self.parse_br_pitching_data()
                except Exception as e:
                    print(e)
                    pass


class Team(DBController):
    def __init__(self, name=None, test=True):
        self._team = name
        super().__init__(test)

    def by_name(self, name):
        self._team = name

    def get_stat(self, stat):
        stat_path = '${}'.format(stat)
        res = self._db.Teams.aggregate([{'$match': {'Tm' : self._team}},
                                        {'$project': {'_id' : 0,
                                                      'stat' : stat_path}}])
        try:
            return list(res)[0]['stat']
        except:
            return None

    def get_stats(self, stats):
        stat_path = {stat : '${}'.format(stat) for stat in stats}
        stat_path.update({'_id' : 0})
        res = self._db.Teams.aggregate([{'$match': {'Tm' : self._team}},
                                        {'$project': stat_path}])

        stat_array = list(res)[0]
        return stat_array

    def get_logo(self, team=None):
        team = self._team if not team else team
        res = self._db.Teams.aggregate([{'$match': {'Tm': team}},
                                        {'$project': {'_id'  : 0,
                                                      'logo' : 1}}])
        try:
            binary_data = list(res)[0]['logo']
            image = Image.frombytes('RGB', (320, 180), binary_data)
            return image
        except:
            return None

    def get_team_division(self):
        return self._db.Teams.find_one({'Tm' : self._team})['div']

    def get_teams_by_division(self, div):
        res =  self._db.Teams.aggregate([{'$match': {'div' : div}},
                                         {'$project': {'_id' : 0,
                                                       'Team' : '$Tm'}}])
        return [team['Team'] for team in list(res)]

    def get_elo_stats(self):
        rating   = '$elo.elo_rating'
        playoff  = '$elo.playoff_pct'
        division = '$elo.division_pct'
        worldser = '$elo.worldseries_pct'
        return self._db.Teams.aggregate([{'$unwind': '$elo'},
                                         {'$project':
                                             {'_id'          : 0,
                                              'Team'         : '$Tm',
                                              'Rating'       : rating,
                                              'Playoff%'     : playoff,
                                              'Division%'    : division,
                                              'WorldSeries%' : worldser}}])

    def get_past_game_dates(self, last_n=10):
        # Get all game dates from schedule
        schedule = self.get_stat('Schedule')
        dates = [x['Date'] for x in schedule]
        dates = [' '.join(date.split()[1:3]) + ' ' + self._year
                                              for date in dates]
        # Format dates
        datefrmt = [datetime.strptime(x, '%b %d %Y').strftime('%Y-%m-%d')
                                                          for x in dates]
        # Find dates before current day
        today = datetime.today().strftime('%Y-%m-%d')
        game_dates = [date for date in datefrmt if date < today]

        # Sort and return last_n dates
        return sorted(game_dates, reverse=True)[:last_n]

    def get_games_behind_history(self):
        res = self._db.Teams.aggregate([{'$match': {'Tm' : self._team}},
                                        {'$unwind': '$Schedule'},
                                        {'$project':
                                            {'_id' : 0,
                                             'Date': '$Schedule.Date',
                                             'GB'  : '$Schedule.GB'}}])
        hist = [x for x in list(res) if 'GB' in x.keys()]
        return hist

    def get_injuries(self):
        res = self._db.Teams.aggregate([{'$match': {'Tm' : self._team}},
                                        {'$project':
                                            {'_id' : 0,
                                             'Injuries': '$Injuries'}}])
        return list(res)[0]['Injuries']

    def get_transactions(self):
        res = self._db.Teams.aggregate([{'$match': {'Tm' : self._team}},
                                        {'$project':
                                            {'_id' : 0,
                                             'txs': '$Transactions'}}])

        return list(res)[0]['txs'][self._year]
