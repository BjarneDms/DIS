
"""Script containing all functions related to writing the output files"""
import math

# Analysis functions:
# For Variance -------------------------------------------------------------
def stddev(time_stamps) -> int:
    """Calculate the standard deviation of timestamps"""
    length = len(time_stamps)
    mean = sum(time_stamps) / length
    var = sum((elem - mean) ** 2 for elem in time_stamps) / length
    var = round(var, ndigits=2)
    return int(math.sqrt(var))

def variancehash_1(variance) -> int:
    return (variance // 10) + 1

def variancehash_2(variance) -> int:
    return ((variance - 5) // 10) +1

# For process length -------------------------------------------------------------
def process_length(from_servers) -> int:
    return len(set(from_servers))

def lengthhash_1(length) -> int:
    return (length//6) + 1

def lengthhash_2(length):
    return ((length-3)//6) + 1



# Output file functions ------------------------------------------------------
def observationfile(part:str, group:dict, logfile:list):
    with open(f'../data/part{part}Observations.txt', 'w') as f:
        for key, value in group.items():
            f.write(f"Group:{{{','.join(map(str, value))}}} \n")
            for elem in value:
                f.write(f"{elem}: \n")
                for log in logfile:
                    if log.get('ID') == elem:
                        f.write(f"<{','.join(map(str, log.values()))}> \n")

def output(group:dict, logfile:list):
    with open('../data/part1Output.txt', 'w') as f:
        i = 1
        first_processes = set(value[0] for value in group.values())
        for key, value in group.items():
            for elem in value:
                if elem in first_processes:
                    for log in logfile:
                        if log.get('ID') == elem:
                            f.write(f"<{','.join(map(str, list(log.values())[:-1])) + ',' + str(i)}> \n")
            i += 1
