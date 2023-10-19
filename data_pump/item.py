import datetime
import logging

import const
from data_pump.sequences import connect_to_db
from data_pump.utils import read_json, convert_response_to_json, do_api_post, \
    save_dict_as_json
from data_pump.var_declarations import DC_RELATION_REPLACES, DC_RELATION_ISREPLACEDBY
from support.dspace_proxy import rest_proxy
from const import API_URL
from migration_const import WORKFLOWITEM_DICT, WORKSPACEITEM_DICT, ITEM_DICT

def import_item(metadata_class,
                handle_class,
                workflowitem_id_dict,
                workspaceitem_id_dict,
                item_id_dict,
                collection_id_dict,
                eperson_id_dict,
                statistics_dict,
                item_handle_item_metadata_dict,
                save_dict):
    """
    Import data into database.
    Mapped tables: item, collection2item, workspaceitem, cwf_workflowitem,
    metadata, handle
    """
    item_json_name = "item.json"
    workspaceitem_json_name = "workspaceitem.json"
    workflowitem_json_name = 'workflowitem.json'
    collection2table_name = "collection2item.json"
    item_url = 'clarin/import/item'
    workflowitem_url = 'clarin/import/workflowitem'
    item2collection_url = 'clarin/import/item/{item_uuid}/mappedCollections'
    imported_workspaceitem = 0
    imported_workflowitem = 0
    imported_item = 0
    # create dict from items by item id
    item_json_list = read_json(item_json_name)
    items_dict = {}
    if not item_json_list:
        logging.info("Item JSON is empty.")
        return
    for item in item_json_list:
        items_dict[item['item_id']] = item
    # statistics_dict['item'] = (len(item_json_list), 0)
    #
    # # create item and workspaceitem
    # workspaceitem_json_list = read_json(workspaceitem_json_name)
    # if workspaceitem_json_list is not None:
    #     for workspaceitem in workspaceitem_json_list:
    #         item = items_dict[workspaceitem['item_id']]
    #         import_workspaceitem(item, workspaceitem['collection_id'],
    #                              workspaceitem['multiple_titles'],
    #                              workspaceitem['published_before'],
    #                              workspaceitem['multiple_files'],
    #                              workspaceitem['stage_reached'],
    #                              workspaceitem['page_reached'],
    #                              metadata_class,
    #                              handle_class,
    #                              workspaceitem_id_dict,
    #                              item_id_dict,
    #                              collection_id_dict,
    #                              eperson_id_dict)
    #         imported_workspaceitem += 1
    #         del items_dict[workspaceitem['item_id']]
    #
    #     statistics_dict['workspaceitem'] = (len(workspaceitem_json_list),
    #                                         imported_workspaceitem)
    #     imported_item += imported_workspaceitem
    #     # save workspaceitem dict as json
    #     if save_dict:
    #         save_dict_as_json(WORKSPACEITEM_DICT, workspaceitem_id_dict)
    #     logging.info("Workspaceitem was successfully imported!")
    # else:
    #     logging.info("Workspaceitem JSON is empty.")
    # # create workflowitem
    # # workflowitem is created from workspaceitem
    # # -1, because the workflowitem doesn't contain this attribute
    # workflowitem_json_list = read_json(workflowitem_json_name)
    # if workflowitem_json_list is not None:
    #     for workflowitem in workflowitem_json_list:
    #         item = items_dict[workflowitem['item_id']]
    #         import_workspaceitem(item, workflowitem['collection_id'],
    #                              workflowitem['multiple_titles'],
    #                              workflowitem['published_before'],
    #                              workflowitem['multiple_files'],
    #                              -1,
    #                              -1,
    #                              metadata_class,
    #                              handle_class,
    #                              workspaceitem_id_dict,
    #                              item_id_dict,
    #                              collection_id_dict,
    #                              eperson_id_dict)
    #         # create workflowitem from created workspaceitem
    #         params = {'id': str(workspaceitem_id_dict[workflowitem['item_id']])}
    #         try:
    #             response = do_api_post(workflowitem_url, params, None)
    #             workflowitem_id_dict[workflowitem['workflow_id']] = \
    #                 response.headers['workflowitem_id']
    #             imported_workflowitem += 1
    #         except Exception as e:
    #             logging.error('POST request ' + workflowitem_url + ' for id: ' +
    #                           str(workflowitem['item_id']) + ' failed. Exception: ' +
    #                           str(e))
    #         del items_dict[workflowitem['item_id']]
    #
    #     # save workflow dict as json
    #     if save_dict:
    #         save_dict_as_json(WORKFLOWITEM_DICT, workflowitem_id_dict)
    #     statistics_val = (len(workflowitem_json_list), imported_workflowitem)
    #     statistics_dict['workflowitem'] = statistics_val
    #     imported_item += imported_workflowitem
    #     logging.info("Cwf_workflowitem was successfully imported!")
    # else:
    #     logging.info("Workflowitem JSON is empty.")
    #
    # # create other items
    # for item in items_dict.values():
    #     item_json_p = {
    #         'discoverable': item['discoverable'],
    #         'inArchive': item['in_archive'],
    #         'lastModified': item['last_modified'],
    #         'withdrawn': item['withdrawn']
    #     }
    #     metadatvalue_item_dict = metadata_class.get_metadata_value(2, item['item_id'])
    #     if metadatvalue_item_dict:
    #         item_json_p['metadata'] = metadatvalue_item_dict
    #     handle_item = handle_class.get_handle(2, item['item_id'])
    #     if handle_item is not None:
    #         item_json_p['handle'] = handle_item
    #     params = {
    #         'owningCollection': collection_id_dict[item['owning_collection']],
    #         'epersonUUID': eperson_id_dict[item['submitter_id']]
    #     }
    #     try:
    #         response = do_api_post(item_url, params, item_json_p)
    #         response_json = convert_response_to_json(response)
    #         item_id_dict[item['item_id']] = response_json['id']
    #         imported_item += 1
    #     except Exception as e:
    #         logging.error('POST request ' + item_url + ' for id: ' +
    #                       str(item['item_id']) + ' failed. Exception: ' + str(e))
    #
    # # Import collection2item table - only items which are mapped in more collections
    # # Add another collection into Item only if another collection is not owning_collection
    # collection2table_json_list = read_json(collection2table_name)
    # coll_2_item_dict = {}
    # items_with_more_colls = {}
    # # Find items which are mapped in more collections and store them into dictionary in this way
    # # {'item_uuid': [collection_uuid_1, collection_uuid_2]}
    # for collection2table in collection2table_json_list:
    #     # Every item should have mapped only one collection - the owning collection except the items which
    #     # are mapped into more collections
    #     item_uuid = item_id_dict[collection2table['item_id']]
    #     collection_uuid = collection_id_dict[collection2table['collection_id']]
    #     if item_uuid in coll_2_item_dict:
    #         # Add another collection into dict to get all collections for current Item
    #         coll_2_item_dict[item_uuid].append(collection_id_dict[collection2table['collection_id']])
    #         # Add item UUID and collection UUID into list in this way {`item_uuid`: `collection_uuid`}
    #         items_with_more_colls[item_uuid] = collection_uuid
    #         continue
    #     coll_2_item_dict[item_uuid] = [collection_uuid]
    #
    # # Call Vanilla REST endpoint which add relation between Item and Collection into the collection2item table
    # for item_with_more_coll_uuid in items_with_more_colls.keys():
    #     # Prepare request URL - replace `{item_uuid}` with current `item_with_more_coll_uuid`
    #     request_url = item2collection_url.replace('{item_uuid}', item_with_more_coll_uuid)
    #
    #     # Prepare request body which should looks like this:
    #     # `"https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_1}" + \n
    #     # "https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_2}"
    #     request_body = []
    #     collection_url = 'core/collections/'
    #     for collection_uuid in coll_2_item_dict[item_with_more_coll_uuid]:
    #         request_body.append(API_URL + collection_url + collection_uuid)
    #
    #     do_api_post(request_url, {}, request_body)
    #
    # # save item dict as json
    # if save_dict:
    #     save_dict_as_json(ITEM_DICT, item_id_dict)
    # statistics_val = (statistics_dict['item'][0], imported_item)
    # statistics_dict['item'] = statistics_val


    # Migrate item versions
    migrate_item_history(metadata_class, items_dict, item_id_dict, eperson_id_dict, item_handle_item_metadata_dict)
    logging.info("Item and Collection2item were successfully imported!")


def migrate_item_history(metadata_class,
                         items_dict,
                         item_id_dict,
                         eperson_id_dict,
                         item_handle_item_metadata_dict):
    c7_dspace = connect_to_db(database=const.CLARIN_DSPACE_7_NAME,
                              host=const.CLARIN_DSPACE_7_HOST,
                              port=const.CLARIN_DSPACE_7_PORT,
                              user=const.CLARIN_DSPACE_7_USER,
                              password=const.CLARIN_DSPACE_7_PASSWORD)

    cursor_c7_dspace = c7_dspace.cursor()
    admin_uuid = get_admin_uuid(cursor_c7_dspace)

    # 1. Create sequence of item versions
    #     Store Items for which the version was created in some list - do not create a version for the same item

    # Test only for one item TODO run it in `for`
    # Get item_id by uuid
    item_id = 3094
    for item in items_dict.values():
        if item['item_id'] != item_id:
            continue
        # Do not process withdrawn items.
        if item['withdrawn'] == 'true':
            continue

        item_uuid = item_id_dict[item_id]
        # Get `dc.relation.replace` and `dc.relation.isreplacedby` metadata value
        # Sequence is order from the first version to the latest
        # item_version_sequence = ['http://hdl.handle.net/11234/1-1464', 'http://hdl.handle.net/11234/LRT-1478', 'http://hdl.handle.net/11234/1-1548', 'http://hdl.handle.net/11234/1-1699', 'http://hdl.handle.net/11234/1-1827', 'http://hdl.handle.net/11234/1-1983', 'http://hdl.handle.net/11234/1-2515', 'http://hdl.handle.net/11234/1-2837', 'http://hdl.handle.net/11234/1-2895', 'http://hdl.handle.net/11234/1-2988', 'http://hdl.handle.net/11234/1-3105', 'http://hdl.handle.net/11234/1-3226', 'http://hdl.handle.net/11234/1-3424', 'http://hdl.handle.net/11234/1-3683', 'http://hdl.handle.net/11234/1-3687', 'http://hdl.handle.net/11234/1-4611', 'http://hdl.handle.net/11234/1-4758', 'http://hdl.handle.net/11234/1-4923']
        item_version_sequence = get_item_version_sequence(item_id, items_dict, metadata_class, item_handle_item_metadata_dict)

        # Insert data into `versionhistory`

        versionhistory_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionhistory', 'versionhistory_id') + 1

        cursor_c7_dspace.execute("INSERT INTO versionhistory(versionhistory_id) VALUES (" +
                                 str(versionhistory_new_id) + ");")
        c7_dspace.commit()
        # Insert data into `versionitem` with `versionhistory` id
        versionitem_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionitem', 'versionitem_id') + 1
        for index, item_version_handle in enumerate(item_version_sequence, 1):
            item_handle_id_dict = item_handle_item_metadata_dict[item_version_handle]
            item_id = item_handle_id_dict['item_id']
            item_uuid = item_id_dict[item_id]
            timestamp = datetime.datetime.now()
            cursor_c7_dspace.execute(f'INSERT INTO public.versionitem(versionitem_id, version_number, version_date, version_summary, versionhistory_id, eperson_id, item_id) VALUES ('
                                     f'{versionitem_new_id}, '
                                     f'{index}, '
                                     f'\'{timestamp}\', '
                                     f'\'\', '
                                     f'{versionhistory_new_id}, '
                                     f'\'{admin_uuid}\', '
                                     f'\'{item_uuid}\');')
            versionitem_new_id += 1
        c7_dspace.commit()





    # 2. According to version sequence insert a record into `versionhistory` and `versionitem` table
    #     Create method which fetch item uuid and person uuid by handle

def get_admin_uuid(cursor):
    # Execute a SQL query to retrieve the last record's ID (assuming 'your_table' is the name of your table)
    cursor.execute(f'SELECT uuid FROM eperson WHERE email like \'{const.user}\'')

    # Fetch the result
    eperson_uuid = cursor.fetchone()

    uuid = ''
    # Check if there is a result and extract the ID
    if eperson_uuid:
        uuid = eperson_uuid[0]
    else:
        logging.error("No eperson records in the table.")

    return uuid
def get_last_id_from_table(cursor, table_name, id_column):
    # Execute a SQL query to retrieve the last record's ID (assuming 'your_table' is the name of your table)
    cursor.execute("SELECT " + id_column + " FROM " + table_name + " ORDER BY " + id_column + " DESC LIMIT 1")

    # Fetch the result
    last_record_id = cursor.fetchone()

    # Default value - the table is empty
    last_id = 1
    # Check if there is a result and extract the ID
    if last_record_id:
        last_id = last_record_id[0]
    else:
        logging.error("No records in the table.")

    # Close the cursor and the database connection
    return last_id


def get_item_version_sequence(item_id,
                              items_dict,
                              metadata_class,
                              item_handle_item_metadata_dict):
    current_item_handle = getMetadataValue(item_id, metadata_class, 'dc.identifier.uri')
    # True = previous; False = newer
    newer_versions = get_item_versions(item_id, current_item_handle, metadata_class, item_handle_item_metadata_dict,
                                          True)
    previous_versions = get_item_versions(item_id, current_item_handle, metadata_class, item_handle_item_metadata_dict,
                                       False)
    previous_versions.reverse()

    return previous_versions + [current_item_handle] + newer_versions


def get_item_versions(item_id, current_item_handle, metadata_class, item_handle_item_metadata_dict, previous_or_newer: bool):
    # True = previous; False = newer
    # Get previous version - fetch metadata value from `dc.relation.replaces`
    # Get newer version - fethc metadata value from `dc.relation.isreplaced.by`
    metadata_field = DC_RELATION_REPLACES
    if previous_or_newer:
        metadata_field = DC_RELATION_ISREPLACEDBY

    list_of_version = []
    current_item_id = item_id
    # current_version is handle of previous or newer item
    # current_version = item_handle_item_metadata_dict[current_item_handle][metadata_field]
    current_version = getMetadataValue(current_item_id, metadata_class, metadata_field)
    while current_version is not None:
        list_of_version.append(current_version)

        current_item_id = item_handle_item_metadata_dict[current_version]['item_id']
        current_version = getMetadataValue(current_item_id, metadata_class, metadata_field)

        # if current_item_id == item_handle_item_metadata_dict[current_version][metadata_field]:
        #     current_version = None
        #     continue
        # Get item_id by the handle
        # current_item_id = item_handle_item_metadata_dict[current_version]['item_id']

        # OLD
        # if metadata_field not in item_handle_item_metadata_dict[current_version].keys():
        #     current_version = None
        #     continue
        # current_version = item_handle_item_metadata_dict[current_version][metadata_field]

    return list_of_version


def get_item_id_by_handle(handle, items_dict, metadata_class):
    return 3084

def getMetadataValue(item_id, metadata_class, metadata_field):
    if item_id is None:
        return None

    # 2 = resource_type = Item
    metadata_values = metadata_class.get_metadata_value(2, item_id)
    # because metadata value are stored in the list
    if metadata_field not in metadata_values:
        return None

    metadata_value_list = metadata_values[metadata_field]
    if metadata_value_list:
        return metadata_value_list[0]['value']
    return None

# def getMetadataValue(metadata_values):
#     return metadata_values['dc.relation.isreplacedby']


def import_workspaceitem(item,
                         owning_collectin_id,
                         multiple_titles,
                         published_before,
                         multiple_files,
                         stagereached,
                         page_reached,
                         metadata_class,
                         handle_class,
                         workspaceitem_id_dict,
                         item_id_dict,
                         collection_id_dict,
                         eperson_id_dict):
    """
    Auxiliary method for import item.
    Import data into database.
    Mapped tables: workspaceitem, metadata, handle
    """
    workspaceitem_url = 'clarin/import/workspaceitem'
    workspaceitem_json_p = {
        'discoverable': item['discoverable'],
        'inArchive': item['in_archive'],
        'lastModified': item['last_modified'],
        'withdrawn': item['withdrawn']
    }
    metadatavalue_item_dict = metadata_class.get_metadata_value(2, item['item_id'])
    if metadatavalue_item_dict is not None:
        workspaceitem_json_p['metadata'] = metadatavalue_item_dict
    handle_workspaceitem = handle_class.get_handle(2, item['item_id'])
    if handle_workspaceitem is not None:
        workspaceitem_json_p['handle'] = handle_workspaceitem
    # the params are workspaceitem attributes
    params = {
        'owningCollection': collection_id_dict[owning_collectin_id],
        'multipleTitles': multiple_titles,
        'publishedBefore': published_before,
        'multipleFiles': multiple_files, 'stageReached': stagereached,
        'pageReached': page_reached,
        'epersonUUID': eperson_id_dict[item['submitter_id']]
    }
    try:
        response = do_api_post(workspaceitem_url, params, workspaceitem_json_p)
        workspaceitem_id = convert_response_to_json(response)['id']
        workspaceitem_id_dict[item['item_id']] = workspaceitem_id
        item_url = API_URL + 'clarin/import/' + str(workspaceitem_id) + "/item"
        try:
            response = rest_proxy.d.api_get(item_url, None, None)
            item_id_dict[item['item_id']] = convert_response_to_json(response)['id']
        except Exception as e:
            logging.error('POST request ' + item_url +
                          ' failed. Exception: ' + str(e))
    except Exception as e:
        logging.error('POST request ' + workspaceitem_url + ' for id: ' +
                      str(item['item_id']) +
                      ' failed. Exception: ' + str(e))

