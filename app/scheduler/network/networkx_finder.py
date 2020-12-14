import networkx as nx

from app.scheduler.models import CondottaNode, CondottaEdge


class NetworkFinder:
    def __init__(self, name):
        self.name = name
        self._create_graph()

    def get_g(self):
        return self.G

    def search_successors(self, start_node):
        return list(nx.bfs_successors(self.G, start_node))

    def search_descendants(self, start_node):
        return list(nx.descendants(self.G, start_node))

    def get_furthest_nodes(self, start_node):
        successors = self.search_successors(start_node)
        all_nodes = [y for x in successors for y in x[1]]
        parent_nodes = [x[0] for x in successors]
        return set(all_nodes) - set(parent_nodes)

    def subgraph_with_hidden_nodes(self, start_node):
        subgraph = nx.restricted_view(self.G, [x.id for x in self._get_hidden_condotta_nodes()], list(self._set_edges()))
        return list(nx.bfs_successors(subgraph, start_node))

    def _create_graph(self):
        self.nodes = list(self._set_nodes())
        self.edges = list(self._set_edges())
        self.G = nx.MultiDiGraph(name=self.name)
        self.G.add_nodes_from(self.nodes)
        self.G.add_edges_from(self.edges)
        return self.G

    def _set_edges(self):
        for edge in self._get_condotta_edges():
            yield edge.source, edge.target, edge.id
            if edge.bidirectional:
                yield edge.target, edge.source, edge.id

    def _set_nodes(self):
        for node in self._get_condotta_nodes():
            yield node.id

    @staticmethod
    def _get_condotta_nodes():
        return CondottaNode.objects.all()

    @staticmethod
    def _get_hidden_condotta_nodes():
        return CondottaNode.objects.all().filter(hidden=True)

    @staticmethod
    def _get_condotta_edges():
        return CondottaEdge.objects.all()
