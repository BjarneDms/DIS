
"""Script containing all functions related to writing the output files"""

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
