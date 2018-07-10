import pickle

elist = {'ARI': [],
         'ATL': [],
         'BAL': [],
         'BOS': [],
         'CHC': [],
         'CHW': [],
         'CIN': [],
         'CLE': [],
         'COL': [],
         'DET': [],
         'HOU': ['aaclarke@gmail.com'],
         'KCR': [],
         'LAA': [],
         'LAD': [],
         'MIA': [],
         'MIL': [],
         'MIN': [],
         'NYM': [],
         'NYY': ['aaclarke@gmail.com'],
         'OAK': [],
         'PHI': [],
         'PIT': [],
         'SDP': [],
         'SEA': [],
         'SFG': [],
         'STL': [],
         'TBR': [],
         'TEX': [],
         'TOR': [],
         'WSN': []}

with open('elist.pkl', 'wb') as f:
      pickle.dump(elist, f)

