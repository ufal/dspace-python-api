import unittest

import requests
from bs4 import BeautifulSoup

import const
from support.dspace_interface.response_map import check_response
from support.dspace_proxy import rest_proxy
from support.item_checking import check_com_col, transform_handle_to_oai_set_id, get_handle, \
    assure_item_from_file, import_items, oai_fail_message
from support.logs import log, Severity


class CMDIFormatTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        check_com_col()

    def test_format_cmdi(self):
        uuid = assure_item_from_file("cmdi_check")
        self.remove_additional_date(uuid)
        handle = get_handle(uuid)
        link = const.OAI_cmdi
        oai_response = requests.get(link + transform_handle_to_oai_set_id(get_handle(const.col_UUID)))
        if oai_response.content is None:
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        if oai_response.status_code == 500:
            log(oai_response.content, Severity.WARN)
            self.fail("Failed to get records for handle " + get_handle(const.col_UUID))
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        if not records:
            self.fail("Did not find any records for " + const.OAI_cmdi)
        the_one = None
        x = open("test/data/cmdi_check.xml", encoding="utf-8")
        got = x.read()
        x.close()
        oai_original = BeautifulSoup(got, features="xml").find("metadata")
        for id_element in oai_original.find("cmd:Resources").find("cmd:ResourceProxyList") \
                .find_all("cmd:ResourceProxy", attrs={"id": "lp_"}):
            id_element.find("cmd:ResourceRef").string.replace_with(
                str(const.FE_url).replace("http://", "https://") + "/handle/" + handle)
        for id_element in oai_original.find_all("cmd:identifier"):
            id_element.string.replace_with(const.FE_url + "/handle/" + handle)
        for id_element in oai_original.find_all("cmd:MdSelfLink"):
            id_element.string.replace_with(
                str(const.FE_url).replace("http://", "https://") + "/handle/" + handle + "@format=cmdi")
        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.find("metadata")
        if the_one is None:
            self.fail(oai_fail_message(handle, link))

        cmd = the_one.find("cmd:CMD")
        cmd.attrs["xmlns:ms"] = "http://www.clarin.eu/cmd/"
        cmd.attrs["xmlns:olac"] = "http://www.clarin.eu/cmd/"

        for date in oai_original.find_all("cmd:MdCreationDate"):
            date.string.replace_with(the_one.find("cmd:MdCreationDate").text)
        self.assertEqual(oai_original, the_one)

    def remove_additional_date(self, uuid):
        itm = rest_proxy.d.get_item(uuid)
        check_response(itm, "receiving item from dspace for check")
        update = False
        do_update = self.remove_most_alphabetical_recent_value_if_contains_more("dc.date.available", uuid, itm.json())
        update = update or do_update
        do_update = self.remove_most_alphabetical_recent_value_if_contains_more("dc.date.accessioned", uuid, itm.json())
        update = update or do_update
        if update:
            import_items()

    @staticmethod
    def remove_most_alphabetical_recent_value_if_contains_more(metadatum, uuid, new_itm):
        old = new_itm["metadata"][metadatum]
        values = []
        for x in old:
            values.append(x["value"])
        if len(values) < 2:
            return False
        old_values = values.copy()
        values.sort()
        idx = old_values.index(values[-1])

        response = rest_proxy.d.api_patch(const.BE_url + "api/core/items/" + str(uuid),
                                          "remove", "/metadata/" + metadatum + "/" + str(idx), None)
        log(response)
        return True
