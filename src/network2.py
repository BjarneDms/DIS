from Server import Server
import random
import numpy as np

depth = [1, 2, 3]               # depht is averagely 3
branching_factor = [1, 2, 3]    # branching factor is averagely 2
to_append_nodes = []
dup_nodes = []
sisters_nodes = []
mean = 50
std_dev = 20
network = []
stop_condition = [i for i in range(1, 200)]
print(stop_condition)

nr_servers = 1

root = Server(0, pred=None)
previous_server = root
branching = random.choice(branching_factor)
for i in range(branching):
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
