import os
import requests
import argparse
from tqdm import tqdm
import sys
import logging

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))

import pump  # noqa: E402
import dspace  # noqa: E402
from project_settings import settings  # noqa: E402
from pump._utils import read_json  # noqa: E402
from pump._item import items  # noqa: E402

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("resource_checker")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Resource policies checker of anonymous view of items')
    parser.add_argument('--temp-item-dict', help='item_dict.json', type=str, default=os.path.join(
        _this_dir, "../../src/__temp/resume/item.json"))
    parser.add_argument('--input-handle-json', help='handle.json', type=str, default=os.path.join(
        _this_dir, "../../input/data/handle.json"))
    args = parser.parse_args()

    _logger.info('Resource policies checker of anonymous view of items')

    if not os.path.exists(args.temp_item_dict):
        _logger.critical(f"File {args.temp_item_dict} does not exist - cannot import.")
        sys.exit(1)

    dspace_be = dspace.rest(
        settings["backend"]["endpoint"],
        settings["backend"]["user"],
        settings["backend"]["password"],
        settings["backend"]["authentication"]
    )

    db_env = settings["db_dspace_5"]
    db5 = pump.db(db_env)

    items_id2uuid = read_json(args.temp_item_dict)["data"]["id2uuid"]

    # create select
    # we want all resource_ids for items
    # where the action is READ
    # which are not workspaces or workflows
    # item exists in item table
    # owning group is Anonymous
    sql = """
    SELECT distinct resource_id FROM public.resourcepolicy
        WHERE resource_type_id = '2' 
        AND action_id IN (0, 9, 10) 
        AND NOT EXISTS (SELECT 'x' FROM public.workspaceitem WHERE 
        public.resourcepolicy.resource_id = public.workspaceitem.item_id)
        AND NOT EXISTS (SELECT 'x' FROM public.workflowitem WHERE 
        public.resourcepolicy.resource_id = public.workflowitem.item_id) 
        AND EXISTS (SELECT 'x' FROM public.item WHERE 
        public.resourcepolicy.resource_id = public.item.item_id) 
        AND epersongroup_id = '0'
    """
    sql = " ".join(x.strip() for x in sql.splitlines() if len(x.strip()) > 0)
    dspace5_item_list = db5.fetch_all(sql)

    _logger.info(f"Count of items with anonymous access: {len(dspace5_item_list)}")

    # get IDs for dspace7 from IDs from dspace5 based on map
    dspace7_item_list = [items_id2uuid[str(x[0])] for x in dspace5_item_list]

    # list od item IDs from dspace7 which can READ Anonymous
    dspace7_item_ids_list = []

    page_size = 50

    # get total pages for search
    # max page size for this request is 100
    js = dspace_be.fetch_search_items(size=page_size)
    pages = js['_embedded']['searchResult']['page']['totalPages']

    # get result from each page
    # we don't get items which are withdrawn or discoverable
    for page in tqdm(range(pages)):
        js = dspace_be.fetch_search_items(page=page, size=page_size)
        objects = js['_embedded']['searchResult']['_embedded']['objects']
        # add each object to result list
        for item in objects:
            dspace7_item_ids_list.append(item['_embedded']['indexableObject']['id'])

    _logger.info(
        f"Count of items with anonymous access in Dspace7: {len(dspace7_item_ids_list)}")

    # compare expected items in dspace5 and got items from dspace7
    # log items, which we cannot find
    notfound = 0
    notfound_but_visible = 0
    found = 0

    for item_uuid in tqdm(dspace7_item_list):
        if item_uuid in dspace7_item_ids_list:
            dspace7_item_ids_list.remove(item_uuid)
            found += 1
            continue

        # check if we really don't have access to item in Dspace7
        try:
            response = dspace_be.fetch_raw_item(item_uuid)
            notfound_but_visible += 1
        except Exception as e:
            _logger.error(f"Item with id: {item_uuid} is not visible in DSpace7, "
                          f"but it is visible in DSpace5! "
                          f"Import of resource policies was incorrect!")
            notfound += 1

    _logger.info(
        f"Visible in dspace5 found in dspace7:[{found}], missing visible in dspace 7: [{notfound_but_visible}], missing in dspace7: [{notfound}]")

    # now in new_item_list are items whose resource_policy
    # was not found in dspace5
    # it could be because in dspace7 is using inheritance for resource policies
    # check if you have access for these items in dspace5
    # based on their handles or there was import error
    item_lindat_url = 'https://lindat.mff.cuni.cz/repository/xmlui/handle/'

    handles = read_json(args.input_handle_json)

    # handle has to be defined for item and item has to exist
    itemuuid2handle = {}
    for h in handles:
        item_uuud = items_id2uuid.get(str(h['resource_id']), None)
        if h['resource_type_id'] == items.TYPE and item_uuud is not None:
            itemuuid2handle[item_uuud] = h['handle']

    # do request to dspace5 for remaining items
    found = 0
    errors = 0
    for item_uuid in tqdm(dspace7_item_ids_list):
        if item_uuid not in itemuuid2handle:
            _logger.critical(f"Item with id {item_uuid} not found")
            continue

        response = requests.get(item_lindat_url + itemuuid2handle[item_uuid])
        if response.ok:
            found += 1
            continue

        errors += 1
        _logger.error(
            f"Item with id {item_uuid} is visible in Dspace7 but not in Dspace5!")

    _logger.info(f"Found in lindat [{found}]")
    if errors > 0:
        _logger.critical("!!!!!!!!!!")
        sys.exit(1)
