import pickle
from itertools import groupby

with open('log_experiments.pkl', 'rb') as f:
    log_experiments = pickle.load(f)

for l in log_experiments:
    print(l)


grouped_tuples = dict()
for key, group in groupby(log_experiments, key=lambda x: x[4]):
    grouped_tuples[key] = list(group)

print(grouped_tuples)