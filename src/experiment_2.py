import itertools
import pickle
from collections import Counter
from itertools import groupby

with open('log_experiments.pkl', 'rb') as f:
    log_experiments = pickle.load(f)

with open('clusters2.pkl', 'rb') as f:
    clusters = pickle.load(f)

servers_per_process = {i: -1 for i in range(len(log_experiments))}

process = 0
for l in log_experiments:
    if l[4] != process:
        process = l[4]
    if servers_per_process[process] == -1:
        servers_per_process[process] = [l[1]]
    else:
        servers_per_process[process].append(l[1])

combinations = list(itertools.combinations(range(process+1), 2))


def jaccard_similarity_counter(set1, set2):
    intersection = set1 & set2
    union = (set1 + set2) - intersection
    return sum(intersection.values()) / sum(union.values()) if len(union) > 0 else 0


to_merge = set()
for combination in combinations:
    counter1 = Counter(servers_per_process[combination[0]])
    counter2 = Counter(servers_per_process[combination[1]])
    jaccard_sim = jaccard_similarity_counter(counter1, counter2)
    if jaccard_sim > 0.5:
        to_merge.add((combination[0],combination[1]))


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if len(union) > 0 else 0

all_part2_merges = set()
for key in clusters.items():
    if len(key[1]) > 1:
        combinations = list(itertools.combinations(key[1], 2))
        for combination in combinations:
            all_part2_merges.add(combination)

all_part2_merges = [(min(a, b), max(a, b)) for a, b in all_part2_merges]

print(to_merge)
print(all_part2_merges)
print(jaccard_similarity(to_merge, all_part2_merges))



