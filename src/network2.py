from Server import Server
import networkx as nx
import pygraphviz
import random
import matplotlib.pyplot as plt
import numpy as np

# ----------------------------------------------------------------------------------------------------------------------
branching_factor = [1, 2, 3]                    # Branching factor is averagely 2
to_append_nodes = []                            # List of nodes that still need to get a child node
dup_nodes = []                                  # Temporary list that keeps track of the duplicate nodes
sisters_nodes = []                              # Temporary list that keeps track of the sister nodes
mean = 50                                       # Mean for timestamp generation
std_dev = 20                                    # Std_deviation for timestamp generation
network = []                                    # List of all the servers
stop_condition = [i for i in range(1, 3)]       # How big the chance is that a node is the end node
initial_branching = 4                          # How many options the user has (how many server paths exist)
amount_of_dup = [2, 3, 4]                       # How many duplicates can exist
amount_of_sisters = [2, 3]                      # How many sister nodes can exist
chance_dup_nodes = [1, 2, 3, 4, 5, 6, 7]              # The chance of getting duplicate nodes
chance_sisters_nodes = [1, 2, 3, 4, 5, 6, 7]          # The chance of getting sister nodes
chance_random_link = [1]                        # Chance of getting a random link from a higher node going down
dup_nodes_names = []                            # List we need to update the pred of the children of the dup nodes
sisters_nodes_names = []                        # List we need to update the pred of the children of the sister nodes
end_nodes = []                                  # List of nodes that do not have children
nr_servers = 1                                  # int that keeps track of number of servers
# ----------------------------------------------------------------------------------------------------------------------


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
                                  response_time=response_time, request_time=request_time))

            successor_update(previous_server, network[len(network) - 1].name)                                                   #
            nr_servers += 1
            dup_nodes.append(network[len(network) - 1])
            dup_nodes_names.append(network[len(network) - 1].name)

        network.append(Server(name=nr_servers, pred=dup_nodes_names,
                              response_time=round(np.random.normal(mean, std_dev), ndigits=2),
                              request_time=round(np.random.normal(mean, std_dev), ndigits=2)))
        successor_update2(dup_nodes, network[len(network) - 1].name)

        nr_servers += 1
        if random.choice(stop_condition) == 1:
            network[len(network) - 1].suc = "Last_node"
        else:
            to_append_nodes.append(network[len(network) - 1])
        for nodes in dup_nodes:
            #nodes.suc = network[len(network) - 1].name
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
                #nodes.suc = network[len(network) - 1].name
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
                successor_update(node_to_go, network[len(network) - 1].name + 100)
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


visualisation(network)

# ----------------------------------------------------------------------------------------------------------------------
endpoint = random.choice(network)
