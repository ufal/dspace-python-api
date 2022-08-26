import json
import os
import const
from support.dspace_interface.models import Item
from support.dspace_proxy import rest_proxy
from support.logs import log, Severity

com_col_checked = False


def check_com_col():
    """
    Check if community and collection for test items are created.
    If they do not exist yet, they will be created.
    This check will always run only once and then it will be skipped(see first lines of method)
    """
    global com_col_checked
    if com_col_checked:
        return
    log("checking if community and collection for test items exist")
    if const.ENABLE_IMPORT_AT_START:
        import_items()
    top_community_exists = False
    collection_exists = False
    comm_list = rest_proxy.get("core/communities/search/top").json()
    comms = comm_list['_embedded']["communities"]
    for cm in comms:
        if cm["name"] == const.COM:
            log("top community found")
            const.com_UUID = cm["uuid"]
            top_community_exists = True

    if not top_community_exists:
        x = open("test/data/com.sample.json")
        community_data = json.load(x)
        x.close()

        result = rest_proxy.d.create_community(None, community_data)
        const.com_UUID = result.uuid

    colls_inside = rest_proxy.get("core/communities/" + const.com_UUID + "/collections").json()
    colls = colls_inside["_embedded"]["collections"]

    for cl in colls:
        if cl["name"] == const.COL:
            log("test collection found")
            const.col_UUID = cl["uuid"]
            collection_exists = True
    if not collection_exists:
        x = open("test/data/col.sample.json")
        collection_data = json.load(x)
        x.close()
        result = rest_proxy.d.create_collection(const.com_UUID, collection_data)
        const.col_UUID = result.uuid
    com_col_checked = True


def import_items():
    """
    Imports items into OAI-PMH. Necessary after adding or changing items.
    Is by far the biggest bottleneck, if called when not necessary, but
    we don't need to run those tests fast.
    """
    log("Importing items into OAI-PMH", Severity.INFO)
    result = os.system(const.import_command)


def assure_item_with_name_suffix(name):
    """
    Check and if necessary create item with specified suffix.
    Created from template found in test/data/itm.sample.json
    """
    check_com_col()
    name = str(name)
    log("checking existence of item with suffix " + name)
    itm_uuid = None
    item_exists = False
    items_inside = rest_proxy.get("discover/search/objects?scope=" + const.col_UUID + "&dsoType=ITEM").json()
    items = items_inside["_embedded"]["searchResult"]["_embedded"]["objects"]
    for item in items:
        i = item["_embedded"]["indexableObject"]
        if i["name"] == const.ITM_prefix + name:
            itm_uuid = i["uuid"]
            item_exists = True
    if not item_exists:
        log("creating item with suffix " + name)
        x = open("test/data/itm.sample.json")
        item_data = json.load(x)
        x.close()
        item_data["metadata"]["dc.title"][0]["value"] = const.ITM_prefix + name
        result = rest_proxy.d.create_item(const.col_UUID, Item(item_data))
        itm_uuid = result.uuid
        import_items()
    return itm_uuid


def assure_item_from_file(filename, postpone=False):
    """
    Assure item from specified file exists.
    Name is checked from file["name"], not from ["metadata"]["dc.title"][0]["value"]!!
    postpone=True does not import it immediately, but note that it should be imported later,
    or will not be visible in OAI at all.
    """
    check_com_col()
    log("Checking item from filename " + filename)
    itm_uuid = None
    item_exists = False
    items_inside = rest_proxy.get("discover/search/objects?scope=" + const.col_UUID + "&dsoType=ITEM").json()
    items = items_inside["_embedded"]["searchResult"]["_embedded"]["objects"]
    x = open("test/data/" + filename + ".json", encoding="utf-8")
    data = json.load(x)
    x.close()
    name = data["name"]
    for item in items:
        i = item["_embedded"]["indexableObject"]
        if i["name"] == name:
            itm_uuid = i["uuid"]
            item_exists = True
    if not item_exists:
        log("creating item from file " + filename)
        result = rest_proxy.d.create_item(const.col_UUID, Item(data))
        itm_uuid = result.uuid
        if not postpone:
            import_items()
    return itm_uuid


def get_handle(uuid):
    """
    Find out handle from UUID.
    """
    raw_response = rest_proxy.get("dso/find?uuid=" + uuid)
    if raw_response is None:
        raise Exception("no object found for uuid " + uuid)
    response = raw_response.json()
    return response["handle"]


def transform_handle_to_oai_set_id(handle, dso_type=const.ItemType.COLLECTION):
    """
    Finds set id from OAI for given handle.
    """
    handle_base = str(handle).replace("/", "_")
    if dso_type == const.ItemType.COLLECTION:
        return "col_" + handle_base
    elif dso_type == const.ItemType.COMMUNITY:
        return "com_" + handle_base
    else:
        return handle_base


def oai_fail_message(handle, link):
    """
    Generates fail message for oai
    """
    return "Did not find expected record in OAI for handle: " + handle \
           + " at link " + link
