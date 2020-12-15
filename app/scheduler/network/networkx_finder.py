import networkx as nx

from app.scheduler.models import CondottaNode, CondottaEdge


class NetworkFinder:
    def __init__(self, name):
        self.name = name
        self.G = nx.MultiDiGraph(name=self.name)
        self._create_graph()

    def get_g(self):
        return self.G

    def search_successors(self, start_node):
        successors = sorted(list(nx.bfs_successors(self.G, start_node)))
        path_cost = self._calculate_cost(self.G, successors)
        return successors, path_cost

    def search_descendants(self, start_node):
        return list(nx.descendants(self.G, start_node))

    def get_boundary_nodes(self, start_node):
        successors, cost = self.search_successors(start_node)
        all_nodes = [y for x in successors for y in x[1]]
        parent_nodes = [x[0] for x in successors]
        return set(all_nodes) - set(parent_nodes), cost

    def subgraph_with_hidden_nodes(self, start_node):
        subgraph = nx.restricted_view(self.G, [x.id for x in self.hidden_nodes], self.edges)
        successors = sorted(list(nx.bfs_successors(subgraph, start_node)))
        return successors, self._calculate_cost(subgraph, successors)

    @staticmethod
    def _calculate_cost(G, list_of_nodes):
        total_cost = 0
        for start_node, end_node in list_of_nodes:
            for node in end_node:
                total_cost += G.edges[start_node, node, 0]['cost']
        return total_cost

    def _create_graph(self):
        self.nodes, self.hidden_nodes = self._get_condotta_nodes()
        self.edges = list(self._set_edges())
        self.G.add_nodes_from(self.nodes)
        return self.G

    def _set_edges(self):
        edges = self._get_condotta_edges()
        for edge in edges:
            self.G.add_edge(edge.source, edge.target, cost=edge.cost)
            if edge.bidirectional:
                self.G.add_edge(edge.target, edge.source, cost=edge.cost)
            yield edge.target, edge.source, edge.id

    @staticmethod
    def _get_condotta_nodes():
        nodes = CondottaNode.objects.all()
        hidden_nodes = [x for x in nodes if x.hidden]
        return nodes, hidden_nodes

    @staticmethod
    def _get_condotta_edges():
        return CondottaEdge.objects.all()
