import io
import unittest

import requests
from bs4 import BeautifulSoup

import const
from support.dspace_interface.models import Item
from support.dspace_interface.response_map import check_response
from support.dspace_proxy import rest_proxy
from support.item_checking import check_com_col, transform_handle_to_oai_set_id, get_handle, \
    assure_item_from_file, oai_fail_message, get_test_soup, import_items, get_name_from_file
from support.logs import log, Severity


class OLACTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        check_com_col()

    def setUp(self):
        if const.on_dev_5:
            raise unittest.SkipTest("local.hiddenButHarvestable not yet supported on dev-5.")

    def test_hidden_but_harvestable(self):
        data_filename = "non-harv"
        uuid = assure_item_from_file(data_filename, postpone=True)
        metadatum = "local.hiddenButHarvestable"
        item = rest_proxy.d.get_item(uuid)
        if metadatum not in item.json()["metadata"]:
            response = rest_proxy.d.api_patch(const.BE_url + "api/core/items/" + str(uuid),
                                          "add", "/metadata/" + metadatum, "hidden")
            check_response(response, "patching; adding local.hiddenButHarvestable")
        name_of_item = get_name_from_file(data_filename)
        anon_req = requests.get(const.API_URL + "discover/search/objects?query=" + name_of_item)
        items_found = anon_req.json()["_embedded"]["searchResult"]["_embedded"]["objects"]
        found_count = len(items_found)
        do_import = False
        if found_count != 0:
            items = []
            for x in items_found:
                items.append(x["hitHighlights"]["dc.title"][0])
            log("Found items!", Severity.WARN)
            log(str(items), Severity.WARN)
            do_import = True
        self.assertEqual(found_count, 0, "Found items but expected hidden! They are not hidden all too well!")

        # upload CMDI file
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
            do_import = True
        if do_import:
            import_items()

        handle = get_handle(uuid)
        link = const.OAI_cmdi
        oai_response = requests.get(link + transform_handle_to_oai_set_id(get_handle(const.col_UUID)))

        check_response(oai_response, "getting cmdi format of item " + name_of_item)
        parsed_oai_response = BeautifulSoup(oai_response.content, features="xml")
        records = parsed_oai_response.findAll("record", recursive=True)
        if not records:
            self.fail("Did not find any records for " + const.OAI_olac)
        the_one = None
        oai_original = get_test_soup("cmdi", find_metadata=False)
        for record in records:
            if record.find("identifier", recursive=True).text.split(":")[-1] == handle:
                the_one = record.find("metadata")
        if the_one is None:
            self.fail(oai_fail_message(handle, link))
        self.assertEqual(oai_original.contents[0], the_one.contents[0], "Returns wrong cmdi")
