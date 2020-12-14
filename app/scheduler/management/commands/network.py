from datetime import datetime

from django.core.management.base import BaseCommand

from app.scheduler.network.networkx_finder import NetworkFinder


class Command(BaseCommand):
    help = "Given a specific xlsx into json"
    configuration_dict = {}

    def add_arguments(self, parser):
        parser.add_argument("start_node", nargs="+", type=int)

    def handle(self, *args, **options):
        print(datetime.now())
        start_node = options["start_node"][0]
        x = NetworkFinder(name="condotta")
        print(f"Successors: {x.search_successors(start_node)}")
        print(f"Descendant: {x.search_descendants(start_node)}")
        print(f"Furthest nodes: {x.get_furthest_nodes(start_node)}")
        print(datetime.now())

