
"""Script containing all functions related to writing the output files"""

def observationfile(part:str, group:dict, logfile:list):
    with open(f'part{part}Observations.txt', 'w') as f:
        for key, value in group.items():
            f.write(f"Group:{{{','.join(map(str, value))}}} \n")
            for elem in value:
                f.write(f"{elem}: \n")
                for log in logfile:
                    if log.get('ID') == elem:
                        f.write(f"<{','.join(map(str, log.values()))}> \n")

