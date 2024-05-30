from Server import Server
import networkx as nx
import pygraphviz
import random
import matplotlib.pyplot as plt
import numpy as np

depth = [1, 2, 3]               # depht is averagely 3
branching_factor = [1, 2, 3]    # branching factor is averagely 2
to_append_nodes = []
dup_nodes = []
sisters_nodes = []
mean = 50
std_dev = 20
network = []
stop_condition = [i for i in range(1, 4)]
initial_branching = 5
amount_of_dup = [2, 3]
chance_dup_nodes = [1, 2, 3, 4]
dup_nodes_names = []
print(stop_condition)

nr_servers = 1

root = Server(0, pred=None)
previous_server = root
for i in range(initial_branching):
    network.append(Server(name=nr_servers, pred=previous_server.name,
                          timestamp=round(np.random.normal(mean, std_dev), ndigits=2)))
    to_append_nodes.append(network[len(network) - 1])
    nr_servers += 1

while len(to_append_nodes) != 0:
    to_append_nodes.sort(key=lambda x: x.name)                             #does not work yet because S9 comes after S10
    previous_server = to_append_nodes[0]
    to_append_nodes.remove(previous_server)
    branching = random.choice(branching_factor)
    for i in range(branching):
        #dup nodes
        if random.choice(chance_dup_nodes) == 1:
            amount_of_node_dup = random.choice(amount_of_dup)
            timestamp = round(np.random.normal(mean, std_dev), ndigits=2)
            for j in range(amount_of_node_dup):
                network.append(Server(name=nr_servers, pred=previous_server.name,
                                      timestamp=timestamp))
                nr_servers += 1
                dup_nodes.append(network[len(network) - 1])
                dup_nodes_names.append(network[len(network) - 1].name)

            network.append(Server(name=nr_servers, pred=dup_nodes_names,
                                  timestamp=round(np.random.normal(mean, std_dev), ndigits=2)))
            nr_servers += 1
            if random.choice(stop_condition) == 1:
                network[len(network) - 1].suc = "Last_node"
            else:
                to_append_nodes.append(network[len(network) - 1])
            for nodes in dup_nodes:
                nodes.suc = network[len(network) - 1].name
                nodes.dup = dup_nodes_names
            dup_nodes = []
            dup_nodes_names = []
        else:
            network.append(Server(name=nr_servers, pred=previous_server.name,
                                  timestamp=round(np.random.normal(mean, std_dev), ndigits=2)))
            nr_servers += 1
            if random.choice(stop_condition) == 1:
                network[len(network) - 1].suc = "Last_node"
            else:
                to_append_nodes.append(network[len(network) - 1])
            if len(stop_condition) != 1:
                stop_condition.pop(len(stop_condition)-1)

    '''if nr_servers > 15:
        to_append_nodes = []'''

for server in network:
    print(server)

def visualisation(network):
    G = nx.DiGraph()
    color_map = []
    for nodes in network:
        if nodes.dup is not None:
            G.add_node(nodes.name)
            color_map.append('orchid')
        else:
            G.add_node(nodes.name)
            color_map.append('c')
    color_map.append('blue')
    for nodes in network:
        if type(nodes.pred) is not int:
            for pred in nodes.pred:
                G.add_edge(nodes.name, pred)
        else:
            G.add_edge(nodes.name, nodes.pred)

    pos = nx.nx_agraph.graphviz_layout(G, prog='dot')
    nx.draw(G, pos, with_labels=True, node_color=color_map, node_size=1000, edge_color='gray',
            font_size=15, font_color='black', font_weight='bold', linewidths=2, edgecolors='black')
    plt.show()


visualisation(network)

