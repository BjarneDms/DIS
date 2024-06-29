
"""Script containing all functions related to writing the output files"""
import math

# Analysis functions:
# For Variance ------------------------------------------------------------


def stddev(time_stamps) -> int:
    """Calculate the standard deviation of timestamps"""
    length = len(time_stamps)
    mean = sum(time_stamps) / length
    squared_dev = [(elem - mean) ** 2 for elem in time_stamps]
    var = sum(squared_dev) / length
    var = round(var, ndigits=0)
    return int(math.sqrt(var))


def reactiontime(time_stamps) -> list:
    sorted_time_stamps = sorted(time_stamps)
    all_diff = []
    for i in range(1,len(sorted_time_stamps)):
        all_diff.append(int(sorted_time_stamps[i] - sorted_time_stamps[i-1]))
    return all_diff


# For process length -------------------------------------------------------------
def process_length(from_servers) -> int:
    return len(set(from_servers))


# Output file functions ------------------------------------------------------
def observationfile(part:str, group:dict, logfile:list):
    with open(f'../part{part}Observations.txt', 'w') as f:
        for key, value in group.items():
            f.write(f"Group:{{{','.join(map(str, value))}}} \n")
            for elem in value:
                f.write(f"{elem}: \n")
                for log in logfile:
                    if log.get('ID') == elem:
                        f.write(f"<{','.join(map(str, log.values()))}> \n")

def output(group:dict, logfile:list):
    with open('../part1Output.txt', 'w') as f:
        i = 1
        first_processes = set(value[0] for value in group.values())
        for key, value in group.items():
            for elem in value:
                if elem in first_processes:
                    for log in logfile:
                        if log.get('ID') == elem:
                            f.write(f"<{','.join(map(str, list(log.values())[:-1])) + ',' + str(elem)}> \n")
            i += 1


def jaccard_similarity(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if len(union) > 0 else 0


def jaccard_similarity_counter(set1, set2):
    intersection = set1 & set2
    union = set1 | set2
    return sum(intersection.values()) / sum(union.values()) if len(union) > 0 else 1

