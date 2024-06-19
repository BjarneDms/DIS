import pickle
from collections import defaultdict
from itertools import groupby

with open('copied_network.pkl', 'rb') as f:
    network = pickle.load(f)

with open('log_experiments.pkl', 'rb') as f:
    log_experiments = pickle.load(f)

with open('clusters.pkl', 'rb') as f:
    clusters = pickle.load(f)

print(clusters.items())

# ------------------------------
# Experiment 1

# can be removed, here i get micks candidate pairs of only branching factor and length
candidate_pairs = [('1', 0), ('1', 1), ('1', 2), ('1', 3), ('1', 4), ('1', 5), ('1', 6), ('1', 7), ('1', 8), ('1', 9)]

grouped_pairs = defaultdict(list)

for bucket, process in candidate_pairs:
    grouped_pairs[bucket].append(process)
# Till here


def create_evaluation_pairs(grouped_pairs):
    evaluation_pairs = []
    for bucket, process in grouped_pairs.items():
        for i in range(len(process)):
            for j in range(i + 1, len(process)):
                if process[i] != process[j]:
                    evaluation_pairs.append((process[i], process[j]))
    return evaluation_pairs


comparisons = create_evaluation_pairs(grouped_pairs)

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
    #print(i)
    #print(len(grouped_tuples[tuples[0]]))
    if i == len(grouped_tuples[tuples[0]]):
        if dict_correct_merges[tuples[0]] is None:
            dict_correct_merges[tuples[0]] = [tuples[1]]
        else:
            dict_correct_merges[tuples[0]].append(tuples[1])

#for l in log_experiments:
    #print(l)

print(dict_correct_merges)

#splitten in missed en fout gegroepeerd
