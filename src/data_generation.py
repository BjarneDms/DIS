import networkx as nx
import random
import matplotlib.pyplot as plt

def generate_random_network(depth, branching_factor):
    G = nx.DiGraph()
    node_count = 1

    def add_nodes(current_depth, parent=None):
        nonlocal node_count
        if current_depth == 0:
            return
        for _ in range(branching_factor):
            node_name = node_count
            G.add_node(node_name)
            if parent:
                G.add_edge(parent, node_name)
            node_count += 1
            add_nodes(current_depth - 1, parent=node_name)

    add_nodes(depth)
    return G

# Example usage
depth = 3
branching_factor = 2
random_network = generate_random_network(depth, branching_factor)

# Draw the network
pos = nx.spring_layout(random_network, seed=42)  # Position nodes using a spring layout
nx.draw(random_network, pos, with_labels=True, node_size=800, node_color="skyblue", font_size=10, font_weight="bold", arrowsize=20)
plt.title(f"Random Network (Depth={depth}, Branching Factor={branching_factor})")
plt.show()
