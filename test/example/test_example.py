"""
This serves as example test
"""
import unittest

from bs4 import BeautifulSoup

from support.dspace_proxy import rest_proxy
from support.item_checking import check_com_col, assure_item_from_file
from support.logs import log


class ExampleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up class for testing

        # This checks if community and collection for test items exist.
        check_com_col()

    def test_one(self):
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
        one = open("test/data/bundle_check.xml", encoding="utf-8")
        got = one.read()
        one.close()
        # When creating BeautifulSoup object, use parameter to specify parser.
        dummy = BeautifulSoup(got, "xml")
        two = open("test/data/cmdi.xml", encoding="utf-8")
        got = two.read()
        two.close()
        dummy_two = BeautifulSoup(got, "xml")


        # this can be used for different objects too
        self.assertEqual(dummy, dummy)

        self.assertNotEqual(dummy_two, dummy)
