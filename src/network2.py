from Server import Server
import networkx as nx
import pygraphviz
import random
import matplotlib.pyplot as plt
import numpy as np
import copy
import json
import inspect
import os

# ----------------------------------------------------------------------------------------------------------------------
branching_factor = [1, 2, 3]                    # Branching factor is averagely 2
to_append_nodes = []                            # List of nodes that still need to get a child node
dup_nodes = []                                  # Temporary list that keeps track of the duplicate nodes
sisters_nodes = []                              # Temporary list that keeps track of the sister nodes
mean = 50                                       # Mean for timestamp generation
mean_start_node = 1000                          # Mean for timestamp generation of user
std_dev = 20                                    # Std_deviation for timestamp generation
std_dev_start_node = 300                        # Std_deviation for timestamp generation of user
network = []                                    # List of all the servers
stop_condition = [i for i in range(1, 400)]       # How big the chance is that a node is the end node
initial_branching = 20                          # How many options the user has (how many server paths exist)
amount_of_dup = [i for i in range(2, 4)]            # How many duplicates can exist
amount_of_sisters = [i for i in range(2, 4)]        # How many sister nodes can exist
chance_dup_nodes = [i for i in range(1, 6)]         # The chance of getting duplicate nodes
chance_sisters_nodes = [i for i in range(1, 6)]     # The chance of getting sister nodes
chance_random_link = [i for i in range(1, 3)]       # Chance of getting a random link from a higher node going down
dup_nodes_names = []                            # List we need to update the pred of the children of the dup nodes
sisters_nodes_names = []                        # List we need to update the pred of the children of the sister nodes
end_nodes = []                                  # List of nodes that do not have children
nr_servers = 1                                  # int that keeps track of number of servers
stop_log = [i for i in range(1, 12)]             # Chance of server failing
amount_of_logs = 100                            # How many tasks were performed (one path from node zero to node zero)
chance_go_back_up = [i for i in range(1, 4)]    # Chance of a server calling more than one server
# ----------------------------------------------------------------------------------------------------------------------

# Function responsible for
def successor_update(previous_server, nr):
    if type(previous_server.suc) is list:
        successors = []
        for suc in previous_server.suc:
            if suc is not None and suc != "Last_node" and suc != nr:
                successors.append(suc)
        successors.append(nr)
        previous_server.suc = successors
    else:
        if previous_server.suc is not None and previous_server.suc != "Last_node":
            previous_server.suc = [previous_server.suc, nr]
        else:
            previous_server.suc = nr


def successor_update2(dup_nodes, nr):
    for dup_node in dup_nodes:  #
        successor_update(dup_node, nr)


# Making the root node (the user), with a variable amount of server nodes (amount of options the user has on the site)
root = Server(0, pred=None)
previous_server = root
for i in range(initial_branching):
    # Part responsible for making duplicate nodes
    if random.choice(chance_dup_nodes) == 1:
        amount_of_node_dup = random.choice(amount_of_dup)
        request_time = round(np.random.normal(mean, std_dev), ndigits=2)
        response_time = round(np.random.normal(mean, std_dev), ndigits=2)
        for j in range(amount_of_node_dup):
            network.append(Server(name=nr_servers, pred=previous_server.name,
                                  response_time=abs(response_time), request_time=abs(request_time)))

            successor_update(previous_server, network[len(network) - 1].name)                                        #
            nr_servers += 1
            dup_nodes.append(network[len(network) - 1])
            dup_nodes_names.append(network[len(network) - 1].name)

        network.append(Server(name=nr_servers, pred=dup_nodes_names,
                              response_time=abs(round(np.random.normal(mean, std_dev), ndigits=2)),
                              request_time=abs(round(np.random.normal(mean, std_dev), ndigits=2))))
        successor_update2(dup_nodes, network[len(network) - 1].name)

        nr_servers += 1
        if random.choice(stop_condition) == 1:
            network[len(network) - 1].suc = "Last_node"
        else:
            to_append_nodes.append(network[len(network) - 1])
        for nodes in dup_nodes:
            nodes.dup = dup_nodes_names
        dup_nodes = []
        dup_nodes_names = []

    # Part responsible for linking servers higher up the hierarchy to other nodes that are not duplicates.
    else:
        network.append(Server(name=nr_servers, pred=previous_server.name,
                              response_time=round(np.random.normal(mean, std_dev), ndigits=2),
                              request_time=round(np.random.normal(mean, std_dev), ndigits=2)))
        to_append_nodes.append(network[len(network) - 1])
        successor_update(previous_server, network[len(network) - 1].name)  #
        nr_servers += 1

# Making the entire network
while len(to_append_nodes) != 0:
    to_append_nodes.sort(key=lambda x: x.name)
    previous_server = to_append_nodes[0]
    to_append_nodes.remove(previous_server)
    branching = random.choice(branching_factor)
    for i in range(branching):
        # Part responsible for making duplicate nodes
        if random.choice(chance_dup_nodes) == 1:
            amount_of_node_dup = random.choice(amount_of_dup)
            request_time = round(np.random.normal(mean, std_dev), ndigits=2)
            response_time = round(np.random.normal(mean, std_dev), ndigits=2)
            for j in range(amount_of_node_dup):
                network.append(Server(name=nr_servers, pred=previous_server.name,
                                      response_time=abs(response_time), request_time=abs(request_time)))
                successor_update(previous_server, network[len(network) - 1].name)  #
                nr_servers += 1
                dup_nodes.append(network[len(network) - 1])
                dup_nodes_names.append(network[len(network) - 1].name)

            network.append(Server(name=nr_servers, pred=dup_nodes_names,
                                  response_time=abs(round(np.random.normal(mean, std_dev), ndigits=2)),
                                  request_time=abs(round(np.random.normal(mean, std_dev), ndigits=2))))
            successor_update2(dup_nodes, network[len(network) - 1].name)

            nr_servers += 1
            if random.choice(stop_condition) == 1:
                network[len(network) - 1].suc = "Last_node"
            else:
                to_append_nodes.append(network[len(network) - 1])
            for nodes in dup_nodes:
                nodes.dup = dup_nodes_names
            dup_nodes = []
            dup_nodes_names = []

        # Part responsible for making sister nodes
        elif random.choice(chance_sisters_nodes) == 1:
            amount_of_node_sister = random.choice(amount_of_sisters)
            for j in range(amount_of_node_sister):
                network.append(Server(name=nr_servers, pred=previous_server.name,
                                      response_time=abs(round(np.random.normal(mean, std_dev), ndigits=2)),
                                      request_time=abs(round(np.random.normal(mean, std_dev), ndigits=2))))
                successor_update(previous_server, network[len(network) - 1].name)  #
                nr_servers += 1
                sisters_nodes.append(network[len(network) - 1])
                sisters_nodes_names.append(network[len(network) - 1].name)

            network.append(Server(name=nr_servers, pred=sisters_nodes_names,
                                  response_time=abs(round(np.random.normal(mean, std_dev), ndigits=2)),
                                  request_time=abs(round(np.random.normal(mean, std_dev), ndigits=2))))
            successor_update2(sisters_nodes, network[len(network) - 1].name)

            nr_servers += 1
            if random.choice(stop_condition) == 1:
                network[len(network) - 1].suc = "Last_node"
            else:
                to_append_nodes.append(network[len(network) - 1])
            #for nodes in sisters_nodes:
                #nodes.suc = network[len(network) - 1].name
            sisters_nodes = []
            sisters_nodes_names = []

        # Part responsible for making new nodes that are not sister or duplicate nodes
        else:
            network.append(Server(name=nr_servers, pred=previous_server.name,
                                  response_time=abs(round(np.random.normal(mean, std_dev), ndigits=2)),
                                  request_time=abs(round(np.random.normal(mean, std_dev), ndigits=2))))
            successor_update(previous_server, network[len(network) - 1].name)                                                   #
            nr_servers += 1
            if random.choice(stop_condition) == 1:
                network[len(network) - 1].suc = "Last_node"
                end_nodes.append(network[len(network) - 1].name)
            else:
                to_append_nodes.append(network[len(network) - 1])
            if len(stop_condition) != 1:
                stop_condition.pop(len(stop_condition)-1)

        # Part responsible for linking servers higher up the hierarchy to other nodes that are not duplicates.
        if random.choice(chance_random_link) == 1:
            node_to_go = random.choice(network)
            if node_to_go.name == network[len(network) - 1].name or node_to_go.dup is not None:
                while node_to_go.name == network[len(network) - 1].name or node_to_go.dup is not None:
                    node_to_go = random.choice(network)
            if node_to_go.name != network[len(network) - 1].pred:
                predecessors = network[len(network) - 1].pred
                if type(predecessors) is list:
                    predecessors = []
                    for pred in network[len(network) - 1].pred:
                        predecessors.append(pred)
                    predecessors.append(node_to_go.name)
                else:
                    predecessors = [predecessors, node_to_go.name]
                successor_update(node_to_go, network[len(network) - 1].name)
                node = network[len(network) - 1]
                network.pop()
                node.pred = predecessors
                network.append(node)

# Printing all the servers
print(root)
for server in network:
    print(server)


# Visualization of our network
def visualisation(network_list):
    g = nx.DiGraph()
    color_map = []
    for nodes in network_list:
        if nodes.dup is not None:
            g.add_node(nodes.name)
            color_map.append('orchid')
        else:
            g.add_node(nodes.name)
            color_map.append('c')
    color_map.append('blue')
    for nodes in network:
        if type(nodes.pred) is not int:
            for pred in nodes.pred:
                g.add_edge(nodes.name, pred)
        else:
            g.add_edge(nodes.name, nodes.pred)

    pos = nx.nx_agraph.graphviz_layout(g, prog='dot')
    nx.draw(g, pos, with_labels=True, node_color=color_map, node_size=400, edge_color='gray',
            font_size=5, font_color='black', linewidths=2, edgecolors='black')
    plt.show()


#visualisation(network)

# ----------------------------------------------------------------------------------------------------------------------


def make_path():
    constructing = True
    network_2 = copy.deepcopy(network)
    route = [(root.name, False)]
    next_server = random.choice(root.suc)
    #root.suc.remove(next_server)
    route.append((next_server, False))
    go_back = False

    while constructing:
        if go_back:
            if random.choice(chance_go_back_up) == 1:
                go_back = False
        if not go_back and random.choice(stop_log) != 1:
            if type(network_2[next_server-1].suc) is int:
                next_server2 = network_2[next_server-1].suc
                network_2[next_server2 - 1].pred = next_server
                network_2[next_server - 1].suc = ["go_back"]
                next_server = next_server2
                route.append((next_server, go_back))
            elif (type(network_2[next_server-1].suc) is str or network_2[next_server-1].suc == ["go_back"]
                  or network_2[next_server-1].suc == []):
                next_server = network_2[next_server-1].pred
                go_back = True
                route.append((next_server, go_back))
            else:
                next_server2 = random.choice(network_2[next_server - 1].suc)
                network_2[next_server2 - 1].pred = next_server
                if type(network_2[next_server - 1].suc) is list:
                    if network_2[next_server2 - 1].dup is None:
                        network_2[next_server - 1].suc.remove(next_server2)
                    else:
                        if next_server2 in network_2[next_server2 - 1].dup:
                            for dup in network_2[next_server2 - 1].dup:
                                network_2[next_server - 1].suc.remove(dup)
                        else:
                            network_2[next_server - 1].suc.remove(next_server2)
                else:
                    #if network_2[next_server - 1].dup is None:
                    network_2[next_server - 1].suc = ["go_back"]
                next_server = next_server2
                route.append((next_server, go_back))
        else:
            next_server = network_2[next_server - 1].pred
            route.append((next_server, go_back))
            go_back = True
        if next_server == 0:
            constructing = False

    return route

#question
#- can it go back to one that it has already been at through another route?
#- impliment that it sometimes does not go all the way up again, you might need another boolean for this

log = []
for j in range(amount_of_logs):
    route = make_path()
    base_time = abs(round(np.random.normal(mean_start_node, std_dev_start_node), ndigits=2))
    for i in range(len(route)-1):
        if i == 0:
            log.append(("S0", f"S{route[1][0]}", base_time, "Request", j))
        else:
            if not route[i+1][1]:
                if route[i][0] == len(network):
                    random_response_time = network[len(network)-1].request_time + np.random.normal(mean, std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i + 1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Request", j))
                    base_time += random_response_time
                else:
                    random_response_time = network[route[i][0]].request_time + np.random.normal(mean, std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i+1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Request", j))
                    base_time += random_response_time
            else:
                if route[i][0] == len(network):
                    random_response_time = network[len(network)-1].response_time + np.random.normal(mean, std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i + 1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Response", j))
                    base_time += random_response_time
                else:
                    random_response_time = network[route[i][0]].response_time + np.random.normal(mean, std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i+1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Response", j))
                    base_time += random_response_time

for l in log:
    print(l)

final_sorted_log = sorted(log, key=lambda x: x[2])
print()

#for l in final_sorted_log:
    #print(l)




"""
for i in range(len(route) - 1):                         # What request and response time you use at the end? solved?
    if i == 0:
        log.append((f'S{route[i].name}', f'S{route[i+1].name}', round(base_time,ndigits=2), "Request", j))
    else:
        log.append((f'S{route[i].name}', f'S{route[i + 1].name}',
                    round(base_time+route[i].request_time, ndigits=2), "Request", j))
        base_time = base_time+route[i].request_time

for i in range(len(route) - 1, 0, -1):
    log.append((f'S{route[i].name}', f'S{route[i-1].name}',
                round(base_time+route[i].response_time, ndigits=2), "Response", j))
    base_time = base_time+route[i].response_time

final_log.append(log)

final_sorted_log = []
for log in final_log:
    for details in log:
        final_sorted_log.append(details)

final_sorted_log = sorted(final_sorted_log, key=lambda x: x[2])
"""

"""
while constructing:
    #if not_first_round:

    if working_back:
        if type(network[route[len(route) - 1] - 1].suc) is list:
            next_server = random.choice(network[route[len(route) - 1] - 1].suc)             # now i have the next server
        else:
            next_server = network[route[len(route) - 1] - 1].suc

    if next_server in route or next_server == "Last_node":
        route_back = route.copy()
        working_back = True
        while working_back:
            print(route)
            if hoi:
                next_server = route_back[len(route_back)-2]
                route_back.remove(next_server)
            hoi = True
            print(next_server)
            print(f'route_back: {route_back}')
            print(f'route: {route}')
            route.append(next_server)
            if len(route_back) == 2:
                working_back = False
                constructing = False
                route.append(0)
            elif random.choice(go_back_up) == 1 and (type(network[next_server-1].suc) is not int and str):
                possible_servers = network[next_server-1].suc
                for possible_server in possible_servers:
                    if possible_server in route:
                        possible_servers.remove(possible_server)
                        print(possible_servers)
                if len(possible_servers) != 0:
                    working_back = False
                    next_server = random.choice(possible_servers)
                else:
                    hoi = False
                    i = 0
                    for pred in route:
                        if pred == next_server:
                            next_server = route[i-1]
                            i += 1
                            print(f'next sercer{next_server}')
                            break
    else:
        route.append(next_server)


print(route)
"""

"""
def go_up(go_on, next_server, route):
    while go_on:
        route.append(next_server.copy())
        route[len(route) - 1].request_time = round((route[len(route) - 1].request_time +
                                                    abs(np.random.normal(0, 10))), ndigits=2)
        if type(next_server.suc) is int:
            next = next_server.suc
        else:
            next = random.choice(next_server.suc)
        if type(next) is not str and network[next - 1] not in route:
            next_server = network[next - 1]
        elif type(next) is not str:
            if network[next - 1] in route:
                print("hallo")
                go_on = False
        else:
            stop = random.choice(stop_log)
            if (next_server.suc == "Last_node" or stop == 1):
                go_on = False
                route.append(next_server.copy())
                route[len(route) - 1].request_time = round((route[len(route) - 1].request_time +
                                                            abs(np.random.normal(0, 10))), ndigits=2)

final_log = []
for j in range(amount_of_logs):                      # Log generation
    route = []
    route.append(root.copy())
    route[len(route) - 1].request_time = abs(round(np.random.normal(mean_start_node, std_dev_start_node), ndigits=2))
    next_server = network[random.choice(root.suc)-1]
    go_on = True
    log_not_done = True

    go_up(go_on, next_server, route)

    dynamic_route = route.copy()

    #go up
    while log_not_done:
        route.append(dynamic_route[len(dynamic_route)-2])
        print(len(dynamic_route))
        print(len(route))
        if len(dynamic_route) > 0:
            dynamic_route.pop(len(dynamic_route)-1)
        if len(dynamic_route) == 1:
            log_not_done = False
        if random.choice(go_back_up) == 1:
            new_servers_number = dynamic_route[len(dynamic_route)-1].suc
            new_servers = []
            if type(new_servers_number) is list:
                for server in new_servers_number:
                    new_servers.append(network[server-1])               # correct index?
                for server in new_servers:
                    if server in route:
                        new_servers.remove(server)
            else:
                new_servers = []
            if len(new_servers) != 0:
                next_server = random.choice(new_servers)
                go_on = True
                go_up(go_on, next_server, route)
                dynamic_route = route.copy()                    #misschien kan dit niet
                for server in route:
                    print(server)

        #go down
        #if #go up
            #prep for go up
            #go up



    print()
    for server in route:
        print(server)
"""
"""
    log = []
    base_time = route[0].request_time

    for i in range(len(route) - 1):                         # What request and response time you use at the end? solved?
        if i == 0:
            log.append((f'S{route[i].name}', f'S{route[i+1].name}', round(base_time,ndigits=2), "Request", j))
        else:
            log.append((f'S{route[i].name}', f'S{route[i + 1].name}',
                        round(base_time+route[i].request_time, ndigits=2), "Request", j))
            base_time = base_time+route[i].request_time

    for i in range(len(route) - 1, 0, -1):
        log.append((f'S{route[i].name}', f'S{route[i-1].name}',
                    round(base_time+route[i].response_time, ndigits=2), "Response", j))
        base_time = base_time+route[i].response_time

    final_log.append(log)

final_sorted_log = []
for log in final_log:
    for details in log:
        final_sorted_log.append(details)

final_sorted_log = sorted(final_sorted_log, key=lambda x: x[2])

print()

data = []
for log in final_sorted_log:
    data.append(log)

for log in final_sorted_log:
    print(log)

for log in final_log:                           # niet nodig uiteindelijk
    print()
    for details in log:
        print(details)



print(f'hoi:{data}')





formatted_data = [
    {f"server_1": item[0], f"server_2": item[1], f"time_stamp": item[2], f"type": item[3], f"ID": item[4]}
    for item in data
]

json_data = json.dumps(formatted_data, indent=4)

# Step 3: Write the serialized function to a JSON file
with open("function.json", "w") as json_file:
    json_file.write(json_data)

print("Function has been written to function.json")
print(os.getcwd())
"""