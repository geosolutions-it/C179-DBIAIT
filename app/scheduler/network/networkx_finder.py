import networkx as nx

from app.scheduler.models import CondottaNode, CondottaEdge


class NetworkFinder:
    def __init__(self, name):
        self.name = name
        self._create_graph()

    def get_g(self):
        return self.G

    @staticmethod
    def get_condotta_nodes():
        return CondottaNode.objects.all()

    @staticmethod
    def get_condotta_edges():
        return CondottaEdge.objects.all()

    def search_successors(self, start_node):
        return list(nx.bfs_successors(self.G, start_node))

    def search_descendants(self, start_node):
        return list(nx.descendants(self.G, start_node))

    def get_furthest_nodes(self, start_node):
        successors = self.search_successors(start_node)
        all_nodes = [y for x in successors for y in x[1]]
        parent_nodes = [x[0] for x in successors]
        return set(all_nodes) - set(parent_nodes)

    def _create_graph(self):
        self.nodes = [node.id for node in self.get_condotta_nodes()]
        self.edges = list(self._set_edges())
        self.G = nx.MultiDiGraph(name=self.name)
        self.G.add_nodes_from(self.nodes)
        self.G.add_edges_from(self.edges)
        return self.G

    def _set_edges(self):
        for edge in self.get_condotta_edges():
            yield edge.source, edge.target
            if edge.bidirectional:
                yield edge.target, edge.source
