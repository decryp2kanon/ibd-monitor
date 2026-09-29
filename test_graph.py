"""Small regression tests for graph display semantics."""

import math
from importlib.machinery import SourceFileLoader
from pathlib import Path
import unittest


graph = SourceFileLoader("ibd_graph", str(Path(__file__).with_name("graph.sh"))).load_module()


class GraphDisplayTest(unittest.TestCase):
    def test_block_speed_is_unavailable_before_block_processing(self):
        self.assertTrue(math.isnan(graph.current_block_rate([float("nan"), 0.0], [0, 0])))

    def test_zero_speed_is_valid_after_block_processing_started(self):
        self.assertEqual(graph.current_block_rate([1.0, 0.0], [1, 1]), 0.0)


if __name__ == "__main__":
    unittest.main()
