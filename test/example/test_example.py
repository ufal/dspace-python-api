"""
This serves as example test
"""
import unittest

from bs4 import BeautifulSoup

from support.dspace_proxy import rest_proxy
from support.item_checking import check_com_col, assure_item_from_file, get_test_soup
from support.logs import log


class ExampleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up class for testing

        # This checks if community and collection for test items exist.
        check_com_col()

    def setUp(self):
        pass
        # if you want to skip those tests, uncomment following line:
        # raise unittest.SkipTest("CMDI format not yet on dev-5.")

    def test_example_one(self):
        value = 1 == 2
        self.assertFalse(value)
        value = "a" == "a"
        self.assertTrue(value)
        x = "abcd"
        y = "abcd"
        self.assertEqual(x, y)
        z = "wxyz"
        self.assertNotEqual(y, z)

        # create item from file and get uuid
        # (since there is postpone=True, this item won't be visible in OAI)
        uuid = assure_item_from_file("itm.sample", postpone=True)

        # get item from dspace rest
        item = rest_proxy.d.get_item(uuid)
        item_json = item.json()
        item_tile_from_metadata = item_json["metadata"]["dc.title"][0]["value"]
        log("Example_test item with title: " + item_tile_from_metadata)

        # In order to compare xml objects, BeautifulSoup objects can be constructed and compared:
        # Use function to obtain soup (soup is name of library, also used as name for objects
        # representing xml document
        dummy = get_test_soup("bundle_check")
        dummy_two = get_test_soup("cmdi")

        # this can be used for different objects too
        self.assertEqual(dummy, dummy)

        self.assertNotEqual(dummy_two, dummy)
