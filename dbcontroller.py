from pymongo import MongoClient
import datetime
import re

from utils import convert_name, find_earlier_date

class DBController:
    def __init__(self, test=True):
        if test:
            address='localhost'
            port=27017
            db='newdb'
            self._client = MongoClient(address, port)
        else:
            url="mongodb://alex:Q8b5^SR5Oh@ds123110-a0.mlab.com:23110,ds123110-a1.mlab.com:23110/heroku_kcpx1gp1?replicaSet=rs-ds123110"
            db='heroku_kcpx1gp1'
            self._client = MongoClient(url)
        self._db = self._client[db]
        self._year = datetime.date.today().strftime('%Y')
        self._day  = datetime.date.today().strftime('%Y-%m-%d')

    def get_player(self, player):
        """
        Query player object by name
        """
        return self._db.Players.find_one({'Name' :
                                          re.compile(player, re.IGNORECASE)})

    def player_exists(self, player):
        """
        Check if player exists in Players collection.
        Returns empty list if no player exists.
        If player exists, returns dict showing if
        br or fg data exists.
        """
        res = self._db.Players.aggregate([{'$match': {'Name': player}},
                                          {'$project':
                                              {'br': {'$ifNull': ['$br', 0]},
                                               'fg': {'$ifNull': ['$fg', 0]}}}
                                          ])
        return list(res)

    def find_team_by_player(self, player, year=None):
        """
        Return team for input player
        Useful when team value does not exist in Player object
        """
        year = self._year if not year else year
        fortypath = 'Fortyman.{}.Name'.format(year)
        res = self._db.Teams.aggregate([{'$match':
                                            {fortypath : player}},
                                        {'$project': {'_id' : 0,
                                                      'Tm' : 1}}])
        try:
            return next(res)['Tm']
        except:
            return None

    def get_player_team(self, player, year=None):
        """
        Checks for team info in two locations
        """
        year = self._year if not year else year
        try:
            team = self.find_team_by_player(player, year)
        except:
            team = self.get_player(player)['Team']

        return team

    def get_player_brid(self, player, team, year=None):
        """
        Return Player BR ID from Teams collection
        Player objects are in Fortyman array
        """
        year = self._year if not year else year
        fortypath = '$Fortyman.{}'.format(year)
        namepath = '$Fortyman.{}.Name'.format(year)
        bidpath  = '$Fortyman.{}.bid'.format(year)

        res = self._db.Teams.aggregate([{'$match': {'Tm': team}},
                                        {'$unwind': fortypath},
                                        {'$project': {'_id' : 0,
                                                      'bid' : bidpath,
                                                      'name': namepath}},
                                        {'$match': {'name': player}}])
        return list(res)[0]['bid']

    def get_pitchers_by_game(self, team, date):
        """
        Return team pitcher data from Games collection
        """
        abbr = convert_name(team, how='abbr')
        pitch = '{}.pitching'.format(abbr)
        return self._db.Games.aggregate([{'$match':
                                             {'$and': [{'date':  date},
                                             {'$or':  [{'home' : abbr},
                                                       {'away' : abbr}]}]}},
                                         {'$project': {'_id' : 0,
                                                       pitch : 1}}])

    def get_player_war_fg(self, player, kind, year):
        """
        Return WAR stats from fangraphs
        """
        if kind == 'batter':
            war  = '$fg.bat.{}.bat_WAR'.format(year)
            off  = '$fg.bat.{}.Off'.format(year)
            def_ = '$fg.bat.{}.Def'.format(year)
            res = self._db.Players.aggregate([{'$match': {'Name' : player}},
                                              {'$project': {'_id' : 0,
                                                            'war' : war,
                                                            'off' : off,
                                                            'def' : def_}}])
        elif kind == 'pitcher':
            war = '$fg.pit.{}.pit_WAR'.format(year)
            res = self._db.Players.aggregate([{'$match': {'Name' : player}},
                                              {'$project': {'_id' : 0,
                                                            'war' : war}}])
        return list(res)[0]

    def get_player_war_br(self, player, kind , year):
        """
        Return WAR stats from baseball-reference
        """
        if kind == 'batter':
            war  = '$br.Batting Value.{}.WAR'.format(year)
            off  = '$br.Batting Value.{}.oWAR'.format(year)
            def_ = '$br.Batting Value.{}.dWAR'.format(year)
            res = self._db.Players.aggregate([{'$match': {'Name' : player}},
                                              {'$project': {'_id' : 0,
                                                            'war' : war,
                                                            'off' : off,
                                                            'def' : def_}}])
        elif kind == 'pitcher':
            war = '$br.Pitching Value.{}.WAR'.format(year)
            war = '$fg.pit.{}.pit_WAR'.format(year)
            res = self._db.Players.aggregate([{'$match': {'Name' : player}},
                                              {'$project': {'_id' : 0,
                                                            'war' : war}}])
        return list(res)[0]

    def get_players_by_team(self, team, year=None):
        """
        Query all player objects given a team
        """
        year = self._year if not year else year
        return self._db.Players.find({'{}.Team'.format(year) : team})

    def get_team(self, team):
        """
        Query team object
        """
        abbr = convert_name(team, how='abbr')
        return self._db.Teams.find_one({'Tm' : abbr})

    def get_teams(self, *teams):
        """
        Return docs for specified teams
        """
        teams = [convert_name(team, how='abbr') for team in teams]
        return self._db.Teams.find({'Tm' : {'$in' : teams}})

    def get_all_teams(self):
        """
        Return all docs in Team collection
        """
        return self._db.Teams.find({})

    def get_games_by_date(self, date):
        """
        Query all game objects by given date
        """
        return self._db.Games.find({'date' : date})

    def get_all_game_previews(self):
        """
        Query all games for current day
        that are designated as "Scheduled"
        """
        state = 'preview.gameData.status.detailedState'
        return self._db.Games.find({'date' : self._day,
                                    state  : 'Scheduled'})

    def get_team_game_preview(self, team, date):
        """
        Query game preview for specific team
        """
        abbr = convert_name(team, how='abbr')
        return self._db.Games.find({'date' : date,
                                    '$or' : [{'home' : abbr},
                                             {'away' : abbr}]})

    def get_team_game_previews(self, team, dates):
        """
        Query game preview for team given list of dates
        """
        abbr = convert_name(team, how='abbr')
        return self._db.Games.find({'date' : {'$in' : dates},
                                    '$or' : [{'home' : abbr},
                                             {'away' : abbr}]})

    # def get_gid_by_date_team(self, team, date):
    #     abbr = convert_name(team, how='abbr')
    #     res = self._db.Games.aggregate([{'$match':
    #                                         {'date' : date,
    #                                            '$or' : [{'home' : abbr},
    #                                                     {'away' : abbr}]}},
    #                                     {'$project': {'_id' : 0,
    #                                                   'gid' : 1}}])
    #     return list(res)[0]['gid']

    def get_all_team_previews(self):
        abbr = convert_name(team, how='abbr')
        year = '^{}'.format(self._year)
        return self._db.Games.find({'$and': [{'date': {'$regex': year}},
                                             {'$or':  [{'home' : abbr},
                                                       {'away' : abbr}]}]})

    def get_all_pitch_dates(self, name, team=None, year=None):
        """
        Return list of dates where pitcher was active
        """
        if not team:
            team = self.get_player_team(name, year)

        abbr = convert_name(team, how='abbr')
        dates = self.get_past_game_dates_by_team(abbr, year)
        active_dates = []

        for date in dates:
            if date == self._day:
                continue
            game = list(self._db.Games.find({'$and': [{'date': date},
                                               {'$or':  [{'home' : abbr},
                                                         {'away' : abbr}]}]}))
            pitchers = [x['Pitching'] for x in game[0][abbr]['pitching']]
            if name in pitchers:
                active_dates.append(date)
        return active_dates

    def get_last_pitch_date(self, name, team=None, year=None):
        """
        Find the last day where a pitcher was active
        """
        abbr = convert_name(team, how='abbr')
        dates = self.get_past_game_dates_by_team(abbr, year)

        for date in dates:
            if date == self._day:
                continue
            game = list(self._db.Games.find({'$and': [{'date': date},
                                               {'$or':  [{'home' : abbr},
                                                         {'away' : abbr}]}]}))
            pitchers = [x['Pitching'] for x in game[0][abbr]['pitching']]
            if name in pitchers:
                return date
        return None

    def get_teams_by_division(self, div):
        return self._db.Teams.find({'div' : div})

    def get_matchup_history(self, *teams):
        """
        Return game data between two teams
        Queries the Games collection
        """
        abbrA = convert_name(teams[0], how='abbr')
        abbrB = convert_name(teams[1], how='abbr')

        return self._db.Games.find({'$or' : [{'home' : abbrA,
                                              'away' : abbrB},
                                             {'home' : abbrB,
                                              'away' : abbrA}]})

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

    def get_starters_or_relievers(self, role, pos, year):
        """
        Return list of  starter or reliver object ids
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

    def get_past_game_dates_by_team(self, team, year=None):
        """
        Return list of dates when input team
        has played during the specified year
        """
        year = self._year if not year else year
        data = self._db.Games.aggregate([{'$match':
                                             {'$or':[{'home' : team},
                                                     {'away' : team}]}},
                                         {'$project': {'_id' : 0,
                                                      'date' : 1}}])
        return sorted([x['date'] for x in data
                    if x['date'].split('-')[0] == year], reverse=True)

    def get_past_game_dates(self, year=None):
        """
        Return set of all date values in
        Games collection for a specified year
        """
        year = self._year if not year else year
        data = list(self._db.Games.aggregate([{'$project':
                                                  {'_id' : 0,
                                                  'date' : 1}}]))
        dates = set([x['date'] for x in data
                  if x['date'].split('-')[0] == year])
        return dates

    def find_outdated_game_dates(self):
        """
        Return dates where games have not been updated.
        Delete games that are postponed but state hasn't changed.
        """
        state = 'preview.gameData.status.abstractGameState'
        old = self._db.Games.find({state : {'$nin' : ['Final']}})
        return set([x['date'] for x in old])

    def get_gid_dates(self, gid):
        """
        Return dates associated with gid
        """
        data = self._db.Games.aggregate([{'$match': {'gid' : gid}},
                                         {'$project': {'_id' : 1,
                                                       'gid' : 1,
                                                       'date': 1}}])
        return list(data)

    def game_is_finished(self, _id):
        """
        Determine if game has ended given ObjectId
        """
        state = '$preview.gameData.status.detailedState'
        data = list(self._db.Games.aggregate([{'$match':
                                                         {'_id' : _id}},
                                              {'$project' :
                                                         {'state' : state}}]))
        if data and data[0]['state'][0] in ['Final', 'Game Over']:
            return True
        else:
            return False

    def query_by_gid(self, gid):
        return self._db.Games.find({'gid' : gid})

    def query_by_gids(self, gids):
        """
        Return Game docs given list of gids
        """
        return self._db.Games.find({'gid' : {'$in': gids}})

    def remove_games(self, remove_this):
        self._db.Games.remove({'gid': {'$in': remove_this}})

