import unittest
from unittest.mock import MagicMock

from django.test import SimpleTestCase
from networkx import NetworkXError

from app.scheduler.models import CondottaNode, CondottaEdge
from app.scheduler.network.networkx_finder import NetworkFinder


class MyTestCase(SimpleTestCase):
    def setUp(self) -> None:
        self.sut = NetworkFinder
        self.sut.get_condotta_nodes = MagicMock(
            return_value=[
                CondottaNode(1),
                CondottaNode(2),
                CondottaNode(3),
                CondottaNode(4),
                CondottaNode(5),
            ]
        )
        self.sut.get_condotta_edges = MagicMock(
            return_value=[
                CondottaEdge(source=1, target=2),
                CondottaEdge(source=1, target=8),
                CondottaEdge(source=1, target=5),
                CondottaEdge(source=2, target=3),
                CondottaEdge(source=3, target=9),
                CondottaEdge(source=7, target=2),
                CondottaEdge(source=4, target=5),
                CondottaEdge(source=5, target=10, bidirectional=True)
            ]
        )

    def test_given_a_node_id_1_should_find_the_correct_successor(self):
        actual = self.sut(name="condotta").search_successors(1)
        expected = [(1, [2, 8, 5]), (2, [3]), (3, [9]), (5, [10])]
        self.assertEqual(sorted(expected), sorted(actual))

    def test_given_a_node_id_1000_should_raise_exception(self):
        with self.assertRaises(NetworkXError):
            self.sut(name="condotta").search_successors(1000)

    def test_given_a_node_id_9_should_return_empty_list(self):
        actual = self.sut(name="condotta").search_successors(9)
        expected = [(9, [])]
        self.assertEqual(expected, actual)

    def test_given_a_node_id_10_should_return_the_bidirectional_edge(self):
        actual = self.sut(name="condotta").search_successors(10)
        expected = [(10, [5])]
        self.assertEqual(expected, actual)

    def test_given_a_node_id_2_should_find_the_correct_successor(self):
        actual = self.sut(name="condotta").search_successors(2)
        expected = [(2, [3]), (3, [9])]
        self.assertEqual(expected, actual)

    def test_given_a_node_id_1_should_find_the_correct_descendant(self):
        actual = self.sut(name="condotta").search_descendants(1)
        expected = [2, 3, 5, 8, 9, 10]
        self.assertListEqual(expected, sorted(actual))

    def test_given_a_node_id_2_should_find_the_correct_descendant(self):
        actual = self.sut(name="condotta").search_descendants(2)
        expected = [3, 9]
        self.assertListEqual(expected, sorted(actual))

    def test_given_a_node_id_1_should_return_only_the_furthest_nodes(self):
        actual = self.sut(name="condotta").get_furthest_nodes(1)
        expected = {8, 9, 10}
        self.assertSetEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
