from Server import Server
import networkx as nx
import random
import matplotlib.pyplot as plt
import numpy as np
import copy
import json
import os
import pickle

# ----------------------------------------------------------------------------------------------------------------------
branching_factor = [1,2]              # Branching factor is averagely 2
to_append_nodes = []                            # List of nodes that still need to get a child node
dup_nodes = []                                  # Temporary list that keeps track of the duplicate nodes
sisters_nodes = []                              # Temporary list that keeps track of the sister nodes
mean = 200                                       # Mean for timestamp generation
mean_start_node = 1000                          # Mean for timestamp generation of user
log_mean = 0
std_dev = 40                                   # Std_deviation for timestamp generation
std_dev_start_node = 300                        # Std_deviation for timestamp generation of user
log_std_dev = 0
network = []                                    # List of all the servers
stop_condition = [i for i in range(1, 2)]       # How big the chance is that a node is the end node
initial_branching = 2                           # How many options the user has (how many server paths exist)
amount_of_dup = [i for i in range(2, 4)]            # How many duplicates can exist
amount_of_sisters = [i for i in range(2, 4)]        # How many sister nodes can exist
chance_dup_nodes = [i for i in range(1, 4)]         # The chance of getting duplicate nodes
chance_sisters_nodes = [i for i in range(1, 6)]     # The chance of getting sister nodes
chance_random_link = [i for i in range(1, 10)]       # Chance of getting a random link from a higher node going down
dup_nodes_names = []                            # List we need to update the pred of the children of the dup nodes
sisters_nodes_names = []                        # List we need to update the pred of the children of the sister nodes
end_nodes = []                                  # List of nodes that do not have children
nr_servers = 1                                  # int that keeps track of number of servers
stop_log = [i for i in range(1, 12)]            # Chance of server failing
amount_of_logs = 20                             # How many tasks were performed (one path from node zero to node zero)
chance_go_back_up = [i for i in range(1, 10)]  # Chance of a server calling more than one server
log_length = 15
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
#print(root)
#for server in network:
   # print(server)


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


visualisation(network)

#copying network for other experiment
copied_network = copy.deepcopy(network)

with open('copied_network.pkl', 'wb') as f:
    pickle.dump(copied_network, f)

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
            go_back = True
            route.append((next_server, go_back))
        if next_server == 0:
            constructing = False

    return route

#question
#- can it go back to one that it has already been at through another route?
#- impliment that it sometimes does not go all the way up again, you might need another boolean for this

not_long_enough = True
log = []
#j = 0                                       #voor log lengte
for j in range(amount_of_logs):            #voor log proceses
#while not_long_enough:                      #voor log lengte
    route = make_path()
    base_time = abs(round(np.random.normal(mean_start_node, std_dev_start_node), ndigits=2))
    for i in range(len(route)-1):
        if i == 0:
            log.append(("S0", f"S{route[1][0]}", base_time, "Request", j))
        else:
            if not route[i+1][1]:
                if route[i][0] == len(network):
                    random_response_time = network[len(network)-1].request_time + np.random.normal(log_mean, log_std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i + 1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Request", j))
                    base_time += random_response_time
                else:
                    random_response_time = network[route[i][0] - 1].request_time + np.random.normal(log_mean, log_std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i+1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Request", j))
                    base_time += random_response_time
            else:
                if route[i][0] == len(network):
                    random_response_time = network[len(network)-1].response_time + np.random.normal(log_mean, log_std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i + 1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Response", j))
                    base_time += random_response_time
                else:
                    random_response_time = network[route[i][0] - 1].response_time + np.random.normal(log_mean, log_std_dev)
                    log.append((f"S{route[i][0]}", f"S{route[i+1][0]}",
                                abs(round(base_time + random_response_time, ndigits=2)), "Response", j))
                    base_time += random_response_time
    print(f'{j}: {route}')

    # voor log lengte
    # -------
    """
    j += 1
    if len(log) > log_length:
        not_long_enough = False
    """
    # -------

for l in log:
    print(l)

for n in network:
    print(n)

final_sorted_log = sorted(log, key=lambda x: x[2])

print(route)

#for l in final_sorted_log:
    #print(l)

#print(final_sorted_log)

log_experiments = copy.deepcopy(log)

with open('log_experiments.pkl', 'wb') as f:
    pickle.dump(log_experiments, f)

formatted_data = [
    {f"server_1": item[0], f"server_2": item[1], f"time_stamp": item[2], f"type": item[3], f"ID": item[4]}
    for item in final_sorted_log
]
json_data = json.dumps(formatted_data, indent=4)

# Step 3: Write the serialized function to a JSON file
with open("../data/logfile.json", "w") as json_file:
    json_file.write(json_data)

# Write the logfile as a .txt file
with open("../data/logfile.txt", 'w') as f:
    for log in formatted_data:
        f.write(f"<{','.join(map(str, list(log.values())))}> \n")

print("Logfile has been written to data")
print(os.getcwd())