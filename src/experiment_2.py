import itertools
import pickle
from collections import Counter
from itertools import groupby
import re
from filefunctions import jaccard_similarity

jac_treshold = 0.5

with open('../part1Output.txt', 'r') as f:
    log_experiments = f.readlines()

with open('clusters2.pkl', 'rb') as f:
    clusters = pickle.load(f)

servers_per_process = {}

for l in log_experiments:
    match = re.match(r'<S(\d+),S(\d+),([\d.]+),(\w+),(\d+)>', l.strip())
    if match:
        server_from, server_to, time, request_type, proc = match.groups()
        server_from = int(server_from)
        server_to = int(server_to)
        time = float(time)
        proc = int(proc)
        if proc not in servers_per_process:
            servers_per_process[proc] = [server_to]
        else:
            servers_per_process[proc].append(server_to)
print(servers_per_process)
processes = list(servers_per_process.keys())
combinations = list(itertools.combinations(processes, 2))
#print(combinations)

to_merge = set()
for combination in combinations:
    servers1 = set(servers_per_process[combination[0]])
    servers2 = set(servers_per_process[combination[1]])
    jaccard_sim = jaccard_similarity(servers1, servers2)
    if jaccard_sim >= jac_treshold:
        to_merge.add((combination[0],combination[1]))

all_part2_merges = set()
for key in clusters.items():
    print(key)
    if len(key[1]) > 1:
        combinations = list(itertools.combinations(key[1], 2))
        for combination in combinations:
            all_part2_merges.add(combination)

new_all_merges = [(min(a, b), max(a, b)) for a, b in to_merge]
new_all_part2_merges = [(min(a, b), max(a, b)) for a, b in all_part2_merges]

new_all_merges = sorted(new_all_merges, key=lambda x: (x[0], x[1]))
new_all_part2_merges = sorted(new_all_part2_merges, key=lambda x: (x[0], x[1]))

intersection = set(new_all_merges).intersection(set(new_all_part2_merges))
proportion = len(intersection)/len(new_all_merges)
print(intersection)
print(round(proportion,2))






