import itertools
import pickle
from collections import Counter
from itertools import groupby
import re

jac_treshold = 0.7

with open('../data/part1Output.txt', 'r') as f:
    log_experiments = f.readlines()

with open('clusters2.pkl', 'rb') as f:
    clusters = pickle.load(f)

servers_per_process = {}

process = 0
for l in log_experiments:
    match = re.match(r'<S(\d+),S(\d+),([\d.]+),(\w+),(\d+)>', l.strip())
    if match:
        server_from, server_to, time, request_type, proc = match.groups()
        server_from = int(server_from)
        server_to = int(server_to)
        time = float(time)
        proc = int(proc)
        if proc != process:
            process += 1
        if process not in servers_per_process:
            servers_per_process[process] = [server_to]
        else:
            servers_per_process[process].append(server_to)
print(servers_per_process)
combinations = list(itertools.combinations(range(process+1), 2))


def jaccard_similarity_counter(set1, set2):
    intersection = set1 & set2
    union = set1 | set2
    return sum(intersection.values()) / sum(union.values()) if len(union) > 0 else 1


to_merge = set()
for combination in combinations:
    counter1 = Counter(servers_per_process[combination[0]])
    counter2 = Counter(servers_per_process[combination[1]])
    jaccard_sim = jaccard_similarity_counter(counter1, counter2)
    if jaccard_sim >= jac_treshold:
        to_merge.add((combination[0],combination[1]))


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if len(union) > 0 else 0

all_part2_merges = set()
for key in clusters.items():
    print(key)
    if len(key[1]) > 1:
        combinations = list(itertools.combinations(key[1], 2))
        for combination in combinations:
            all_part2_merges.add(combination)

all_part2_merges = [(min(a, b), max(a, b)) for a, b in all_part2_merges]

print(to_merge)
print(all_part2_merges)
print(jaccard_similarity(to_merge, all_part2_merges))



