import asyncio
import io
import unittest

import requests
from bs4 import BeautifulSoup

import const
from support.dspace_interface.models import Item
from support.dspace_proxy import rest_proxy
from support.item_checking import check_com_col, transform_handle_to_oai_set_id, get_handle, \
    assure_item_from_file, import_items, oai_fail_message, get_test_soup
from support.logs import log, Severity


class CMDIBundleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        check_com_col()

    def setUp(self):
        if const.on_dev_5:
            raise unittest.SkipTest("CMDI format not yet on dev-5.")

    def test_bundle_cmdi_in_OAI(self):
        uuid = assure_item_from_file("bundle_check", postpone=True)
        item = rest_proxy.d.get_item(uuid)
        bundle = None
        new_bundle_name = "METADATA"
        try:
            bundle = rest_proxy.d.create_bundle(Item(item.json()), new_bundle_name)
        except ConnectionError:
            # Bundle already exists
            bundles = rest_proxy.d.get_bundles(Item(item.json()))
            for bun in bundles:
                if bun.name == new_bundle_name:
                    bundle = bun
                    break
        streams = rest_proxy.d.get_bitstreams(bundle=bundle)
        if len(streams) == 0:
            x = open("test/data/cmdi.xml", encoding="utf-8")
            got = x.read()
            x.close()
            stream = io.StringIO(got)

            rest_proxy.d.create_bitstream(bundle, "cmdi_file", stream, mime="text/xml")
            import_items()

        handle = get_handle(uuid)
        col_handle = get_handle(const.col_UUID)
        link = const.OAI_cmdi + transform_handle_to_oai_set_id(col_handle)
        oai_response = requests.get(link)
        fail_message = oai_fail_message(link, col_handle)
        if oai_response.content is None:
            self.fail(fail_message)
        if oai_response.status_code == 500:
            log(oai_response.content, Severity.WARN)
            self.fail(fail_message)
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        if not records:
            self.fail(fail_message)
        the_one = None
        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.metadata.contents[0]
        if the_one is None:
            self.fail(oai_fail_message(handle, link))
        oai_original = get_test_soup("cmdi", find_metadata=False)
        self.assertEqual(oai_original.contents[0], the_one)
