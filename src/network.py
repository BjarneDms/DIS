from Server import Server

#global depth
global dup_rate
global dup_count
global num_servers
global time_diff_dup_servers
#global branching_factor
global time_diff_servers
#global initial_branching_factor
#global list_servers

global chance_final_server
global list_dup_servers

starting_servers = 0
server_count = 1
depth = 0
initial_branching_factor = 1
branching_factor = 2
list_servers = []
while initial_branching_factor != starting_servers:
    while depth != 3:
        for i in range(branching_factor):
            if server_count - i == 1:
                list_servers.append(Server(server_count, pred=0))
            else:
                list_servers.append(Server(server_count, pred=(server_count - i - 1)))

            server_count += 1
        depth += 1
    starting_servers += 1

for server in list_servers:
    print(server)



