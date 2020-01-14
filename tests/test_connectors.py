import os
import tarfile
import unittest

from ayeaye.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector
from ayeaye.connectors.csv_connector import CsvConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_FLOWERPOT_PATH = os.path.join(PROJECT_TEST_PATH,
                                      'data',
                                      'exampleflowerpot.tar.gz'
                                      )

EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH,
                                'data',
                                'deadly_creatures.csv'
                                      )
EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'


class TestConnectors(unittest.TestCase):

    def test_flowerpot_deserialize(self):
        test_string = bytes(
            '{"availability": "apple", "referential": "raspberry"}\n{"availability": "anchor", "referential": "rudder"}',
            encoding='utf-8')
        result = FlowerpotEngine._deserialize_ndjson_string(test_string)
        assert result[0] == {"availability": "apple", "referential": "raspberry"}
        assert result[1] == {"availability": "anchor", "referential": "rudder"}

    def test_iterate_over_json_lines(self):
        with tarfile.open(EXAMPLE_FLOWERPOT_PATH, 'r:gz') as tf:
            reader = FlowerpotEngine(tf)
            results = list(reader.items())
            assert len(results) == 4
            assert "availability" in results[0]
            assert "referential" in results[0]

    def test_flowerpot_all_items(self):
        """
        Iterate all the data items in all the files in the example flowerpot.
        """
        c = FlowerPotConnector(engine_url="flowerpot://"+EXAMPLE_FLOWERPOT_PATH)
        all_items = [(r.availability, r.referential) for r in c]
        all_items.sort()
        expected = "[('acoustic', 'rap'), ('anchor', 'rudder'), ('antenna', 'receive'), ('apple', 'raspberry')]"
        assert expected == str(all_items)

    def test_flowerpot_query_one_file(self):
        """
        The 'table' kwarg gets rows from all files that start with that string.
        """
        c = FlowerPotConnector(engine_url="flowerpot://"+EXAMPLE_FLOWERPOT_PATH)
        some_items = [(r.availability, r.referential) for r in c.query(table='test_a')]
        some_items.sort()
        expected = "[('anchor', 'rudder'), ('apple', 'raspberry')]"
        assert expected == str(some_items)

    def test_csv_basics(self):
        """
        Iterate all the data items and check each row is being yielded as an instance of :class:`ayeaye.Pinnate`
        """
        c = CsvConnector(engine_url="csv://"+EXAMPLE_CSV_PATH)

        animals_names = ", ".join([deadly_animal.common_name for deadly_animal in c])
        expected = 'Crown of thorns starfish, Golden dart frog'
        assert expected == str(animals_names)
