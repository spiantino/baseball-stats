from pymongo import MongoClient
import datetime
import re

from utils import convert_name, find_earlier_date

class DBController:
    def __init__(self, test=True):
        if test:
            address='localhost'
            port=27017
            db='mlbDB'
            self._client = MongoClient(address, port)
        else:
            url="mongodb://alex:Q8b5^SR5Oh@ds123110-a0.mlab.com:23110,ds123110-a1.mlab.com:23110/heroku_kcpx1gp1?replicaSet=rs-ds123110"
            db='heroku_kcpx1gp1'
            self._client = MongoClient(url)
        self._db = self._client[db]
        self._current_year = datetime.date.today().strftime('%Y')
        self._current_day  = datetime.date.today().strftime('%Y-%m-%d')

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

    def get_player_brid(self, player, team, year=None):
        """
        Return Player BR ID from Teams collection
        Player objects are in Fortyman array
        """
        year = self._current_year if not year else year
        namepath = '$Fortyman.{}.Name'.format(year)
        bidpath  = '$Fortyman.{}.bid'.format(year)

        res = self._db.Teams.aggregate([{'$match': {'Tm': team}},
                                        {'$unwind': '$Fortyman.2018'},
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
        pitch = '{}.pitching'.format(team)
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
        year = self._current_year if not year else year
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
        return self._db.Games.find({'date' : self._current_day,
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

    def get_games_behind_history(self, team):
        """
        Queries the Schedule doc from Teams collection
        Returns the Date and Games Behind values
        """
        abbr = convert_name(team, how='abbr')
        res = self._db.Teams.aggregate([{'$match': {'Tm' : abbr}},
                                        {'$unwind': '$Schedule'},
                                        {'$project':
                                            {'_id' : 0,
                                             'Date' : '$Schedule.Date',
                                             'GB': '$Schedule.GB'}}])
        hist = [x for x in list(res) if 'GB' in x.keys()]
        return hist

    def get_top_n_leaders(self, kind, stat, year, n):
        """
        Query top 10 batting or pitching leaderboard
        """
        if stat in ['WAR', 'rank', 'G']:
            sort_key = '{0}.{1}.{1}_{2}'.format(year, kind, stat)
        else:
            sort_key = '{}.{}.{}'.format(year, kind, stat)

        lb = self._db.Players.find({}).sort(sort_key, -1).limit(n)

        return [x['fg'][kind][str(year)] for x in lb]

    def get_top_n_homerun_leaders(self, year, n):
        sort_key = '{}.bat.HR'.format(year)
        return self._db.Players.find({}).sort(sort_key, -1)

    def get_starters_or_relievers(self, role, kind, year):
        """
        Return list of  starter or reliver object ids
        """
        cond    = '$lt' if role=='reliever' else '$gt'
        gp      = 'fg.{}.{}.pit_G'.format(kind, year)
        war_val = '$fg.{}.{}.pit_WAR'.format(kind, year)
        gp_val  = '$fg.{}.{}.pit_G'.format(kind, year)
        gs_val  = '$fg.{}.{}.GS'.format(kind, year)

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

        kind: 'starter' or 'reliever'
        stat: 'pit_WAR', 'ERA', or any other stat
        """
        stat = 'pit_WAR' if stat == 'WAR' else stat
        sort_key = 'fg.pit.{}.{}'.format(year, stat)
        ids = self.get_starters_or_relievers(role=role, year=year, kind='pit')
        sort_direction = 1 if ascending else -1

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

    # def get_pitching_results(self, team, date):
    #     """
    #     Get starting pitcher stats
    #     """
    #     abbr = convert_name(team, how='abbr')
    #     return self._db.Teams.aggregate({'$match' : {}})

    def get_past_game_dates(self):
        """
        Return set of all date values in Games collection
        """
        data = list(self._db.Games.aggregate([{'$project':
                                                  {'_id' : 0,
                                                  'date' : 1}}]))
        dates = set([x['date'] for x in data])
        return dates

    def get_missing_array_dates(self, array):
        """
        Return dates from docs in Game collection
        where no 'preview' array exists
        """
        data =  self._db.Games.aggregate([{'$match':
                                              {array : {'$exists' : False}}},
                                          {'$project':
                                              {'_id' : 0,
                                               'date': 1}}])
        dates = set([x['date'] for x in data])
        return dates

    # def delete_postponed_game_docs(self):
    #     """529995
    #     Delete Game doc if game has been postponed
    #     but game state has not changed from "Scheduled"
    #     """


    def find_outdated_game_dates(self):
        """
        Return dates where games have not been updated.
        Delete games that are postponed but state hasn't changed.
        """
        state = 'preview.gameData.status.detailedState'
        old = self._db.Games.find({state : {'$nin' : ['Final']}})
        return set([x['date'] for x in old])

    def find_duplicate_game_docs(self):
        """
        Find documents with the same gid.
        This happens when a game is postponed
        and played at a later date.
        """
        gids = self._db.Games.aggregate([{'$group':
                                             {'_id'   : '$gid',
                                              'count' : {'$sum' : 1}}},
                                         {'$match':
                                             {'count' : {'$gt' : 1}}}])
        return [x['_id'] for x in gids]

    def delete_duplicate_game_docs(self):
        """
        Delete duplicate document with earlier dates
        """
        gids = self.find_duplicate_game_docs()
        for gid in gids:
            games = list(self._db.Games.aggregate([{'$match':
                                                       {'gid' : gid}},
                                                   {'$project':
                                                       {'date': 1}}]))
            delete = find_earlier_date(games)
            self._db.Games.remove(delete)

    def query_by_gids(self, gids):
        """
        Return Game docs given list of gids
        """
        return self._db.Games.find({'gid' : {'$in': gids}})

    def remove_games(self, remove_this):
        self._db.Games.remove({'gid': {'$in': remove_this}})

