import unittest

import requests
from bs4 import BeautifulSoup

import const
from support.item_checking import assure_item_from_file, get_handle, check_com_col, get_test_soup
from support.logs import log


class RedirectTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not const.on_dev_5:
            raise unittest.SkipTest("Redirects are only implemented on dev-5. Can only run on dev-5. Skipping")
        check_com_col()

    pass

    def test_header_redirect(self):
        uuid = assure_item_from_file("itm.sample")
        handle = get_handle(uuid)
        going_to = const.FE_url + "/handle/" + handle
        log("Going to" + going_to)
        page = requests.get(going_to, headers={"Accept": "cmdi+xml"})
        # print("got to", page.url)
        page_soup = BeautifulSoup(page.text, "html.parser")
        template_soup = get_test_soup("works", "html", "html.parser", False)
        self.assertEqual(template_soup, page_soup)

    def test_param_redirect(self):
        uuid = assure_item_from_file("itm.sample")
        handle = get_handle(uuid)
        going_to = const.FE_url + "/handle/" + handle + "?format=cmdi"
        page = requests.get(going_to)
        # print("got to", page.url)
        page = BeautifulSoup(page.text, 'html.parser')
        original = get_test_soup("works", "html", "html.parser", False)
        self.assertEqual(original, page)
