import importlib
import unittest


class ImportSmokeTests(unittest.TestCase):
    def test_package_can_be_imported(self) -> None:
        module = importlib.import_module("full_data_extraction_for_btc")
        self.assertEqual(module.__name__, "full_data_extraction_for_btc")
