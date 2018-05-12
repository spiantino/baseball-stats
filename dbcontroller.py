from pymongo import MongoClient
import datetime
import re

from utils import convert_name

class DBController:
    def __init__(self, address='localhost', port=27017, db='mlbDB'):
        self._client = MongoClient(address, port)
        self._db = self._client[db]
        self._current_year = datetime.date.today().strftime('%Y')
        self._current_day  = datetime.date.today().strftime('%Y-%m-%d')

    def get_player(self, player):
        """
        Query player object by name
        """
        return self._db.Players.find_one({'Name' :
                                          re.compile(player, re.IGNORECASE)})

    def get_player_war(self, player, type, year):
        """
        Return WAR stats for player
        """
        if type == 'batter':
            war  = '${}.bat_WAR'.format(year)
            off  = '${}.Off'.format(year)
            def_ = '${}.Def'.format(year)
            res = self._db.Players.aggregate([{'$match': {'Name' : player}},
                                              {'$project': {'_id' : 0,
                                                            'war' : war,
                                                            'off' : off,
                                                            'def' : def_}}])
        elif type == 'pitcher':
            war = '${}.pit_WAR'.format(year)
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

    # def get_game_history(self, team):
    #     """
    #     Return game histories
    #     Queries the Schedule doc from Teams collection
    #     """
    #     abbr = convert_name(team, how='abbr')
    #     return dbc._db.Teams.aggregate([{'$match': {'Tm' : abbr}},
    #                                     {'$project':
    #                                                 {'_id'      : 0,
    #                                                  'Schedule' : 1}}])

    def get_top_n_leaders(self, kind, stat, year, n):
        """
        Query top 10 batting or pitching leaderboard
        """
        if stat in ['WAR', 'rank', 'G']:
            sort_key = '{}.{}_{}'.format(year, kind, stat)
        else:
            sort_key = '{}.{}'.format(year, stat)

        lb = self._db.Players.find({}).sort(sort_key, -1).limit(n)

        return [x[str(year)] for x in lb]

    def get_top_n_homerun_leaders(self, year, n):
        sort_key = '{}.HR'.format(year)
        return self._db.Players.find({}).sort(sort_key, -1)

    def get_starters_or_relievers(self, role, year):
        """
        Return list of  starter or reliver object ids
        """
        cond    = '$lt' if role=='reliever' else '$gt'
        gp      = '{}.pit_G'.format(year)
        war_val = '${}.pit_WAR'.format(year)
        gp_val  = '${}.pit_G'.format(year)
        gs_val  = '${}.GS'.format(year)

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
        sort_key = '{}.{}'.format(year, stat)
        ids = self.get_starters_or_relievers(role, year=year)
        sort_direction = 1 if ascending else -1

        res = self._db.Players.find({'_id' : {'$in' : ids}})\
                              .sort(sort_key, sort_direction).limit(n)
        return [x[str(year)] for x in res]

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
        Return dates form docs in Game collection
        where no 'preview' array exists
        """
        data =  self._db.Games.aggregate([{'$match':
                                              {array : {'$exists' : False}}},
                                          {'$project':
                                              {'_id' : 0,
                                               'date' : 1}}])
        dates = set([x['date'] for x in data])
        return dates






