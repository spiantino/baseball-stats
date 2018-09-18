"""
- resolve db conflicts

Postponed games get MOVED to rescheduled date on baseball-reference, and their game state changes to 'Postponed'.

Suspended games DO NOT get moved - listed in boxscores as original date. Gamestate shows 'Final'

- preserve baseball-reference ordering

missing array dates (no boxscore data on br for date):
- 2018-06-18 = NYY issue noted above
- 2018-07-17 = AL vs NL game

Random errors:
- 2018-08-04 = KCR vs MIN game state is 'Game Over' instead of final
- 2018-07-23 = PIT vs CLE game state is 'Completed Early'
    * using abstractGameState insetad of detailedState in find_outdated_game_docs() fixes this

Use this class in place of dbc.find_oudated_game_dates, dbc.delete_duplicate_game_docs, etc...
"""
from dbcontroller import DBController
import datetime

class ConflictResolver(DBController):
    def __init__(self):
        super().__init__()

    def remove_allstar_game(self):
        asg = list(self._db.Games.aggregate([{'$match':
                                             {'$or':  [{'home' : 'AL'},
                                                       {'away' : 'AL'}]}},
                                             {'$project': {'_id' : 1}}]))
        if asg:
            id_ = asg['_id']
            self._db.Games.remove(id_)

    def compare_dates(self, list_with_dates, how='earlier'):
        """
        Return object id that contains the earlier date
        Input list contains dicts with '_id' and 'date' keys
        """
        id1 = list_with_dates[0]['_id']
        id2 = list_with_dates[1]['_id']

        date1 = list_with_dates[0]['date']
        date2 = list_with_dates[1]['date']

        dt1 = datetime.datetime.strptime(date1, '%Y-%m-%d')
        dt2 = datetime.datetime.strptime(date2, '%Y-%m-%d')

        if how == 'earlier':
            return (id1, date1) if dt1 < dt2 else (id2, date2)
        else:
            return (id1, date1) if dt1 > dt2 else (id2, date2)

    def find_duplicate_game_docs(self):
        """
        Find documents with the same gid.
        This happens when a game is postponed
        and played at a later date.
        """
        data = self._db.Games.aggregate([{'$group':
                                             {'_id'   : '$gid',
                                              'count' : {'$sum' : 1}}},
                                         {'$match':
                                             {'count' : {'$gt' : 1}}}])
        return [x['_id'] for x in data]

    # def delete_duplicate_game_docs(self):
    #     """
    #     Delete duplicate document with earlier date
    #     """
    #     gids = [x[0] for x in self.find_duplicate_game_docs()]
    #     for gid in gids:
    #         games = list(self._db.Games.aggregate([{'$match':
    #                                                    {'gid' : gid}},
    #                                                {'$project':
    #                                                    {'date': 1}}]))
    #         delete = compare_dates(games)
    #         self._db.Games.remove(delete)

    def get_missing_array_dates(self, array):
        """
        Return dates from docs in Game collection
        where no input array exists ('summary', 'preview', etc...)
        """
        data =  self._db.Games.aggregate([{'$match':
                                              {array : {'$exists' : False}}},
                                          {'$project':
                                              {'_id' : 0,
                                               'date': 1}}])
        dates = set([x['date'] for x in data])
        return dates

    def get_postponed_games(self):
        # state = 'preview.gameData.status.detailedState'
        data = self._db.Games.aggregate([{'$match':
                                              {'state' : 'Postponed'}},
                                         {'$project': {'_id' : 1,
                                                       'gid' : 1,
                                                       'date': 1 }}])
        return list(data)

    def remove_postponed_games(self):
        """
        Remove games that were postponed to a later date
        """
        postponed = self.get_postponed_games()
        for game in postponed:
            _id, gid, date = game['_id'], game['gid'], game['date']

            # Log and remove postponed games
            self._db.Removed.insert({'gid'  : gid,
                                     'date' : date,
                                     'reason' : 'postponed'})

            self._db.Games.remove(_id)

    def remove_suspended_games(self):
        """
        Remove games that were resumed from an earlier date
        Earlier game doc will contain the full boxscore data
        """
        postponed = self.get_postponed_games()
        duplicates = self.find_duplicate_game_docs()

        if len(postponed) == len(duplicates):
            return

        postponed_gids = set([x['gid'] for x in postponed])

        # Assuming all duplicates not postponed are suspended
        suspended = list(set(duplicates) - postponed_gids)

        # Log and remove suspended games
        for gid in suspended:
            dates = self.get_gid_dates(gid)
            _id, date = self.compare_dates(dates, how='later')

            self._db.Removed.insert({'gid' : gid,
                                     'date': date,
                                     'reason': 'suspended'})

            # Don't remove a game that is still in progress
            if self.game_is_finished(_id):
                self._db.Games.remove(_id)

    def run(self):
        self.remove_postponed_games()
        self.remove_suspended_games()
