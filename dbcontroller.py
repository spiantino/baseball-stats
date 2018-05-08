from pymongo import MongoClient
import datetime

from scrape import convert_name # move this to utils file

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
        player = ' '.join([x.capitalize() for x in player.split()])
        return self._db.Players.find_one({'Name' : player})

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

    def get_all_teams(self):
        """
        Return all docs in Team collection
        """
        return self._db.Teams.find({})

    def get_games_by_date(self, date):
        """
        Query all game objects by given date

        !!! format input date to %Y-%m-%d
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
        abbr =  convert_name(team, how='abbr')
        # if state == 'Scheduled':
        #     home = 'preview.gameData.teams.home.abbreviation'
        #     away = 'preview.gameData.teams.away.abbreviation'
        # else:
        #     home = 'preview.gameData.teams.home.name.abbrev'
        #     away = 'preview.gameData.teams.away.name.abbrev'

        return self._db.Games.find({'date' : date,
                                    '$or' : [{'home' : abbr},
                                             {'away' : abbr}]})

    def get_matchup_history(self, *teams):
        """
        Return matchup history between two teams
        """
        abbrA = convert_name(teams[0], how='abbr')
        abbrB = convert_name(teams[1], how='abbr')

        return self._db.Games.find({'$or' : [{'home' : abbrA,
                                              'away' : abbrB},
                                             {'home' : abbrB,
                                              'away' : abbrA}]})

    def get_game_history(self, team):
        """
        Return game histories of one team
        """
        pass


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

    def get_starters_or_relievers(self, kind, year):
        """
        Return list of  starter or reliver object ids
        """
        cond    = '$lt' if kind=='reliever' else '$gt'
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

    def get_top_n_pitchers(self, kind, year, by, ascending, n):
        """
        Return top n pitchers by stat

        kind: 'starter' or 'reliever'
        by: 'pit_WAR', 'ERA', or any other stat
        """
        sort_key = '{}.{}'.format(year, by)
        ids = self.get_starters_or_relievers(kind=kind, year=year)
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





