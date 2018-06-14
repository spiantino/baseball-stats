from dbcontroller import DBController

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

    def get_stat(self, site, year, pos, stat):
        stat_path = '${}.{}.{}.{}'.format(site, pos, year, stat)
        res = self._db.Players.aggregate([{'$match': {'Name' : self._name}},
                                          {'$project': {'_id' : 0,
                                                        'stat' : stat_path}}])
        try:
            return list(res)[0]['stat']
        except:
            return None


class Game(DBController):
    def __init__(self, test=True):
        self.reset_internals()

        super().__init__(test)

    def reset_internals(self):
        self._game_details = None
        self._starting_pitcher_data = None
        self._batters = None

    def get_game_preview(self):
        return self._game

    def query_game_preview_by_date(self, team, date):
        self.reset_internals()

        games = self._db.Games.find({'date' : date,
                                     '$or' : [{'home' : team},
                                              {'away' : team}]})
        self._game = self.extract_game(games)['preview'][0]
        self._state = self._game['gameData']['status']['detailedState']

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

            if any([state == 'Final' for state in status]):
                idx = [status.index(x) for x in status if x!='Final'][0]
                return games[idx]

            else:
                idx = compare_game_times(games)
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

    def parse_game_details(self):
        game_data = self._game['gameData']
        home = game_data['teams']['home']
        away = game_data['teams']['away']

        if self._state == 'Scheduled':
            home_name = home['name']
            away_name = away['name']
            home_abbr = home['abbreviation']
            away_abbr = away['abbreviation']
            home_rec  = home['record']['leagueRecord']
            away_rec  = away['record']['leagueRecord']

            raw_game_time = game_data['datetime']['time']
            hour = int(raw_game_time.split(':')[0]) + 1
            mins = raw_game_time.split(':')[1]
            game_time = "{}:{}".format(hour, mins)

            wind_dir = ''

        else:
            home_name = home['name']['full']
            away_name = away['name']['full']
            home_abbr = home['name']['abbrev']
            away_abbr = away['name']['abbrev']
            home_rec  = home['record']
            away_rec  = away['record']
            game_time = game_data['datetime']['time']

            wind_dir = game_data['weather']['wind']

        home_wins = home_rec['wins']
        away_wins = away_rec['wins']

        home_losses = home_rec['losses']
        away_losses = away_rec['losses']

        am_or_pm = game_data['datetime']['ampm']
        stadium = game_data['venue']['name']

        self._game_details = {'homeName' : home_name,
                              'homeAbbr' : home_abbr,
                              'homeWins' : home_wins,
                              'homeLoss' : home_losses,
                              'awayName' : away_name,
                              'awayAbbr' : away_abbr,
                              'awayWins' : away_wins,
                              'awayLoss' : away_losses,
                              'gameTime' : game_time,
                              'windDir'  : wind_dir,
                              'am_or_pm' : am_or_pm,
                              'stadium'  : stadium }

    def parse_starting_pitchers(self):
        game_data = self._game['gameData']
        live_data = self._game['liveData']

        if self._state == 'Scheduled':
            pitchers = game_data['probablePitchers']
            players = game_data['players']

            away_pit_id = 'ID' + str(pitchers['away']['id'])
            home_pit_id = 'ID' + str(pitchers['home']['id'])

            away_pit_data = players[away_pit_id]
            home_pit_data = players[home_pit_id]

            away_pit_name = away_pit_data['fullName']
            home_pit_name = home_pit_data['fullName']

            away_pit_num = away_pit_data['primaryNumber']
            home_pit_num = home_pit_data['primaryNumber']

            away_pit_hand = away_pit_data['pitchHand']['code']
            home_pit_hand = home_pit_data['pitchHand']['code']

        else:
            pitchers = live_data['boxscore']['teams']
            players  = live_data['players']['allPlayers']

            away_pit_id = 'ID' + str(pitchers['away']['pitchers'][0])
            home_pit_id = 'ID' + str(pitchers['home']['pitchers'][0])

            away_pit_data = players[away_pit_id]
            home_pit_data = players[home_pit_id]

            away_pit_name = ' '.join((away_pit_data['name']['first'],
                                      away_pit_data['name']['last']))

            home_pit_name = ' '.join((home_pit_data['name']['first'],
                                      home_pit_data['name']['last']))

            away_pit_num = away_pit_data['shirtNum']
            home_pit_num = home_pit_data['shirtNum']

            away_pit_hand = away_pit_data['rightLeft']
            home_pit_hand = home_pit_data['rightLeft']

        self._starting_pitcher_data = {'home': [home_pit_name,
                                                home_pit_hand,
                                                home_pit_num],
                                       'away': [away_pit_name,
                                                away_pit_hand,
                                                away_pit_num]}

    def parse_batters(self):
        live_data = self._game['liveData']['boxscore']
        away_players = live_data['teams']['away']['players']
        home_players = live_data['teams']['home']['players']

        if self._state == 'Scheduled':
            away_batter_ids = away_players.keys()
            home_batter_ids = home_players.keys()

            away_names = [away_players[pid]['person']['fullName']
                                      for pid in away_batter_ids]

            home_names = [home_players[pid]['person']['fullName']
                                      for pid in home_batter_ids]

            away_pos = [away_players[pid]['position']['abbreviation']
                                          for pid in away_batter_ids]

            home_pos = [home_players[pid]['position']['abbreviation']
                                          for pid in home_batter_ids]

            away_num = [away_players[pid]['jerseyNumber']
                              for pid in away_batter_ids]

            home_num = [home_players[pid]['jerseyNumber']
                              for pid in home_batter_ids]

            away_batters = list(zip(away_names, away_pos, away_num))
            home_batters = list(zip(home_names, home_pos, home_num))

            away_bench = None
            home_bench = None


        else:
            away_starters = live_data['teams']['away']['battingOrder']
            home_starters = live_data['teams']['home']['battingOrder']

            away_bench = live_data['teams']['away']['bench']
            home_bench = live_data['teams']['home']['bench']

            away_batter_ids = ['ID{}'.format(x) for x in away_starters]
            home_batter_ids = ['ID{}'.format(x) for x in home_starters]

            away_bench_ids = ['ID{}'.format(x) for x in away_bench]
            home_bench_ids = ['ID{}'.format(x) for x in home_bench]

            away_batter_data = [away_players[pid] for pid in away_batter_ids]
            home_batter_data = [home_players[pid] for pid in home_batter_ids]

            away_bench_data  = [away_players[pid] for pid in away_bench_ids]
            home_bench_data =  [home_players[pid] for pid in home_bench_ids]

            def extract_data(data):
                names = [' '.join((batter['name']['first'],
                                   batter['name']['last']))
                                        for batter in data]
                posn = []
                for batter in data:
                    try:
                        posn.append(batter['position'])
                    except:
                        posn.append(None)

                nums = [batter['shirtNum'] for batter in data]

                return list(zip(names, posn, nums))

            away_batters = extract_data(away_batter_data)
            home_batters = extract_data(home_batter_data)

            away_bench = extract_data(away_bench_data)
            home_bench = extract_data(home_bench_data)


        self._batters = {'away_batters' : away_batters,
                         'home_batters' : home_batters,
                         'away_bench'   : away_bench,
                         'home_bench'   : home_bench}


    def get_starting_pitchers(self):
        if not self._starting_pitcher_data:
            self.parse_starting_pitchers()

        return self._starting_pitcher_data

    def get_game_details(self):
        if not self._game_details:
            self.parse_game_details()

        return self._game_details

    def get_batters(self):
        if not self._batters:
            self.parse_batters()

        return self._batters

class Team(DBController):
    def __init__(self, test=True):
        super().__init__(test)
