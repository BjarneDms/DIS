import pickle
from collections import defaultdict
from itertools import groupby
import itertools
import json

with open('copied_network.pkl', 'rb') as f:
    network = pickle.load(f)

with open('log_experiments.pkl', 'rb') as f:
    log_experiments = pickle.load(f)

with open('clusters1.pkl', 'rb') as f:
    clusters = pickle.load(f)

# ------------------------------
# Experiment 1
with open("../length_branch_buckets.json", "r") as r:
    length_branch_buckets = json.load(r)


def create_evaluation_pairs(grouped_pairs):
    evaluation_pairs = []
    for bucket, process in grouped_pairs.items():
        for i in range(len(process)):
            for j in range(i + 1, len(process)):
                if process[i] != process[j]:
                    evaluation_pairs.append((process[i], process[j]))
    return evaluation_pairs


comparisons = create_evaluation_pairs(length_branch_buckets)

filtered_comparisons = [(min(a, b), max(a, b)) for a, b in comparisons]
filtered_comparisons = [comp for comp in filtered_comparisons if len(comp) == 2]
filtered_comparisons = list(set(filtered_comparisons))
filtered_comparisons = sorted(filtered_comparisons, key=lambda x: x[0])

sorted_log = []
for l in log_experiments:
    sorted_log.append((l[0],l[1],l[4]))

sorted_log = sorted(sorted_log, key=lambda x: x[2])

grouped_tuples = {}
for key, group in groupby(sorted_log, key=lambda x: x[2]):
    grouped_tuples[key] = list(group)

dict_correct_merges = {i: None for i in range(len(grouped_tuples))}
all_merges = set()

for tuples in filtered_comparisons:
    i = 0
    error_found = False
    while not error_found and i != len(grouped_tuples[tuples[0]]):
        if len(grouped_tuples[tuples[0]]) != len(grouped_tuples[tuples[1]]):
            error_found = True
        elif grouped_tuples[tuples[0]][i][1] != grouped_tuples[tuples[1]][i][1]:
            server_number = grouped_tuples[tuples[0]][i][1].lstrip('S')
            server_number = int(server_number)
            if network[server_number-1].dup != None:
                if int(grouped_tuples[tuples[1]][i][1].lstrip('S')) not in network[server_number-1].dup:
                    error_found = True
            else:
                error_found = True
        i += 1

    if i == len(grouped_tuples[tuples[0]]):
        all_merges.add((tuples[0], tuples[1]))

    '''
    if i == len(grouped_tuples[tuples[0]]):
        if dict_correct_merges[tuples[0]] is None:
            dict_correct_merges[tuples[0]] = [tuples[1]]
        else:
            dict_correct_merges[tuples[0]].append(tuples[1])
    '''
all_part1_merges = set()
for key in clusters.items():
    if len(key[1]) > 1:
        combinations = list(itertools.combinations(key[1], 2))
        for combination in combinations:
            all_part1_merges.add(combination)

all_part1_merges = [(min(a, b), max(a, b)) for a, b in all_part1_merges]


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if len(union) > 0 else 1


new_all_merges = [(min(a, b), max(a, b)) for a, b in all_merges]
new_all_part1_merges = [(min(a, b), max(a, b)) for a, b in all_part1_merges]

new_all_merges = sorted(new_all_merges, key=lambda x: (x[0], x[1]))
new_all_part1_merges = sorted(new_all_part1_merges, key=lambda x: (x[0], x[1]))

# print(new_all_merges)
# print(new_all_part1_merges)

print(f'The Jaccard similarity is: {round(jaccard_similarity(set(new_all_merges), set(new_all_part1_merges)), 2)}')

