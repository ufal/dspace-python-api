import unittest
from bs4 import BeautifulSoup
import requests

import const
from support.dspace_proxy import rest_proxy
from support.item_checking import assure_item_with_name_suffix, check_com_col, transform_handle_to_oai_set_id, \
    get_handle, \
    assure_item_from_file, oai_fail_message
from support.logs import log, Severity


class OpenaireTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # raise unittest.SkipTest("Not yet implemented")
        check_com_col()

    def test_see_openaire(self):
        uuid_non = assure_item_with_name_suffix("non-openaire")
        uuid_opn = assure_item_from_file("yes-openaire")
        handle_non = get_handle(uuid_non)
        handle_opn = get_handle(uuid_opn)
        link = const.OAI_openaire_dc
        col_handle = get_handle(const.col_UUID)
        oai_response = requests.get(link + transform_handle_to_oai_set_id(col_handle))
        err_msg = oai_fail_message(col_handle, link)
        if oai_response.content is None:
            self.fail(err_msg)
        if oai_response.status_code == 500:
            log(oai_response.content, Severity.WARN)
            self.fail(err_msg)
        parsed_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_response.find("ListRecords", recursive=True)
        if records is None:
            self.fail(err_msg)
        handles_in_oai = []
        for record in records:
            handle = record.find('header').find('identifier').text.split(":")[-1]
            self.assertNotEqual(handle_non, handle)
            handles_in_oai.append(handle)
        self.assertIn(handle_opn, handles_in_oai)

    def test_format_check_openaire_oai_dc(self):
        uuid = assure_item_from_file("openaire_check")
        handle = get_handle(uuid)
        link = const.OAI_openaire_dc
        oai_response = requests.get(link + transform_handle_to_oai_set_id(get_handle(const.col_UUID)))
        if oai_response.content is None:
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        if oai_response.status_code == 500:
            log(oai_response.content, Severity.WARN)
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        the_one = None
        x = open("test/data/openaire_check.oai_dc.xml", encoding="utf-8")
        got = x.read()
        x.close()
        oai_original = BeautifulSoup(got, features="xml").find("metadata")
        for id_element in oai_original.find_all("dc:identifier"):
            if str(id_element.text).__contains__("handle"):
                id_element.string.replace_with(const.FE_url + "/handle/" + handle)

        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.find("metadata")
        if the_one is None:
            self.fail(oai_fail_message(handle, link))
        self.assertEqual(oai_original, the_one)

    def test_format_check_openaire_oai_datacite(self):
        uuid = assure_item_from_file("openaire_check")
        item = rest_proxy.d.get_item(uuid)
        actual_handle = item.json()["metadata"]["dc.identifier.uri"][0]["value"]
        fail_because_hdl_handle_net = "hdl.handle.net" not in str(actual_handle)
        if fail_because_hdl_handle_net:
            self.fail("Handles are issued as " + str(actual_handle) +
                      " and not in format http://hdl.handle.net/{handle}")
        handle = get_handle(uuid)
        link = const.OAI_openaire_datacite
        oai_response = requests.get(link + transform_handle_to_oai_set_id(get_handle(const.col_UUID)))
        if oai_response.content is None:
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        if oai_response.status_code == 500:
            log(oai_response.content, Severity.WARN)
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        the_one = None
        x = open("test/data/openaire_check.oai_datacite.xml", encoding="utf-8")
        got = x.read()
        x.close()
        oai_original = BeautifulSoup(got, features="xml").find("metadata")
        for id_element in oai_original.find_all("identifier", attrs={"identifierType": "Handle"}):
            id_element.string.replace_with(handle)

        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.find("metadata")
        if the_one is None:
            self.fail(oai_fail_message(handle, link))
        self.assertEqual(oai_original, the_one)
