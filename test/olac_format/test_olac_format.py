import unittest

import requests
from bs4 import BeautifulSoup

import const
from support.dspace_interface.response_map import check_response
from support.item_checking import check_com_col, transform_handle_to_oai_set_id, get_handle, \
    assure_item_from_file, oai_fail_message, get_test_soup


class OLACTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        check_com_col()

    def setUp(self):
        if const.on_dev_5:
            raise unittest.SkipTest("OLAC format not yet on dev-5.")

    def test_format_olac(self):
        uuid = assure_item_from_file("olac_check")
        handle = get_handle(uuid)
        link = const.OAI_olac
        oai_response = requests.get(link + transform_handle_to_oai_set_id(get_handle(const.col_UUID)))

        check_response(oai_response, "getting olac item")
        # if oai_response.content is None:
        #     self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        # if oai_response.status_code == 500:
        #     log(oai_response.content, Severity.WARN)
        #     self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        if not records:
            self.fail("Did not find any records for " + const.OAI_olac)
        the_one = None
        oai_original = get_test_soup("olac_check.olac")
        for id_element in oai_original.find_all("dc:identifier"):
            if str(id_element.text).__contains__("handle"):
                id_element.string.replace_with(const.FE_url + "/handle/" + handle)
        for id_element in oai_original.find_all("dcterms:bibliographicCitation"):
            if str(id_element.text).__contains__("handle"):
                id_element.string.replace_with(const.FE_url + "/handle/" + handle)
        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.find("metadata")
        if the_one is None:
            self.fail(oai_fail_message(handle, link))
        self.assertEqual(oai_original, the_one)
