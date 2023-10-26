import datetime
import logging

import const
from data_pump.sequences import connect_to_db
from data_pump.utils import read_json, convert_response_to_json, do_api_post, \
    save_dict_as_json
from data_pump.var_declarations import DC_RELATION_REPLACES_ID, DC_RELATION_ISREPLACEDBY_ID, DC_IDENTIFIER_URI_ID
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
    statistics_dict['item'] = (len(item_json_list), 0)

    # create item and workspaceitem
    workspaceitem_json_list = read_json(workspaceitem_json_name)
    if workspaceitem_json_list is not None:
        for workspaceitem in workspaceitem_json_list:
            item = items_dict[workspaceitem['item_id']]
            import_workspaceitem(item, workspaceitem['collection_id'],
                                 workspaceitem['multiple_titles'],
                                 workspaceitem['published_before'],
                                 workspaceitem['multiple_files'],
                                 workspaceitem['stage_reached'],
                                 workspaceitem['page_reached'],
                                 metadata_class,
                                 handle_class,
                                 workspaceitem_id_dict,
                                 item_id_dict,
                                 collection_id_dict,
                                 eperson_id_dict)
            imported_workspaceitem += 1
            del items_dict[workspaceitem['item_id']]

        statistics_dict['workspaceitem'] = (len(workspaceitem_json_list),
                                            imported_workspaceitem)
        imported_item += imported_workspaceitem
        # save workspaceitem dict as json
        if save_dict:
            save_dict_as_json(WORKSPACEITEM_DICT, workspaceitem_id_dict)
        logging.info("Workspaceitem was successfully imported!")
    else:
        logging.info("Workspaceitem JSON is empty.")
    # create workflowitem
    # workflowitem is created from workspaceitem
    # -1, because the workflowitem doesn't contain this attribute
    workflowitem_json_list = read_json(workflowitem_json_name)
    if workflowitem_json_list is not None:
        for workflowitem in workflowitem_json_list:
            item = items_dict[workflowitem['item_id']]
            import_workspaceitem(item, workflowitem['collection_id'],
                                 workflowitem['multiple_titles'],
                                 workflowitem['published_before'],
                                 workflowitem['multiple_files'],
                                 -1,
                                 -1,
                                 metadata_class,
                                 handle_class,
                                 workspaceitem_id_dict,
                                 item_id_dict,
                                 collection_id_dict,
                                 eperson_id_dict)
            # create workflowitem from created workspaceitem
            params = {'id': str(workspaceitem_id_dict[workflowitem['item_id']])}
            try:
                response = do_api_post(workflowitem_url, params, None)
                workflowitem_id_dict[workflowitem['workflow_id']] = \
                    response.headers['workflowitem_id']
                imported_workflowitem += 1
            except Exception as e:
                logging.error('POST request ' + workflowitem_url + ' for id: ' +
                              str(workflowitem['item_id']) + ' failed. Exception: ' +
                              str(e))
            del items_dict[workflowitem['item_id']]

        # save workflow dict as json
        if save_dict:
            save_dict_as_json(WORKFLOWITEM_DICT, workflowitem_id_dict)
        statistics_val = (len(workflowitem_json_list), imported_workflowitem)
        statistics_dict['workflowitem'] = statistics_val
        imported_item += imported_workflowitem
        logging.info("Cwf_workflowitem was successfully imported!")
    else:
        logging.info("Workflowitem JSON is empty.")

    # create other items
    for item in items_dict.values():
        item_json_p = {
            'discoverable': item['discoverable'],
            'inArchive': item['in_archive'],
            'lastModified': item['last_modified'],
            'withdrawn': item['withdrawn']
        }
        metadatvalue_item_dict = metadata_class.get_metadata_value(2, item['item_id'])
        if metadatvalue_item_dict:
            item_json_p['metadata'] = metadatvalue_item_dict
        handle_item = handle_class.get_handle(2, item['item_id'])
        if handle_item is not None:
            item_json_p['handle'] = handle_item
        params = {
            'owningCollection': collection_id_dict[item['owning_collection']],
            'epersonUUID': eperson_id_dict[item['submitter_id']]
        }
        try:
            response = do_api_post(item_url, params, item_json_p)
            response_json = convert_response_to_json(response)
            item_id_dict[item['item_id']] = response_json['id']
            imported_item += 1
        except Exception as e:
            logging.error('POST request ' + item_url + ' for id: ' +
                          str(item['item_id']) + ' failed. Exception: ' + str(e))

    # Import collection2item table - only items which are mapped in more collections
    # Add another collection into Item only if another collection is not owning_collection
    collection2table_json_list = read_json(collection2table_name)
    coll_2_item_dict = {}
    items_with_more_colls = {}
    # Find items which are mapped in more collections and store them into dictionary in this way
    # {'item_uuid': [collection_uuid_1, collection_uuid_2]}
    for collection2table in collection2table_json_list:
        # Every item should have mapped only one collection - the owning collection except the items which
        # are mapped into more collections
        item_uuid = item_id_dict[collection2table['item_id']]
        collection_uuid = collection_id_dict[collection2table['collection_id']]
        if item_uuid in coll_2_item_dict:
            # Add another collection into dict to get all collections for current Item
            coll_2_item_dict[item_uuid].append(collection_id_dict[collection2table['collection_id']])
            # Add item UUID and collection UUID into list in this way {`item_uuid`: `collection_uuid`}
            items_with_more_colls[item_uuid] = collection_uuid
            continue
        coll_2_item_dict[item_uuid] = [collection_uuid]

    # Call Vanilla REST endpoint which add relation between Item and Collection into the collection2item table
    for item_with_more_coll_uuid in items_with_more_colls.keys():
        # Prepare request URL - replace `{item_uuid}` with current `item_with_more_coll_uuid`
        request_url = item2collection_url.replace('{item_uuid}', item_with_more_coll_uuid)

        # Prepare request body which should looks like this:
        # `"https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_1}" + \n
        # "https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_2}"
        request_body = []
        collection_url = 'core/collections/'
        for collection_uuid in coll_2_item_dict[item_with_more_coll_uuid]:
            request_body.append(API_URL + collection_url + collection_uuid)

        do_api_post(request_url, {}, request_body)

    # save item dict as json
    if save_dict:
        save_dict_as_json(ITEM_DICT, item_id_dict)
    statistics_val = (statistics_dict['item'][0], imported_item)
    statistics_dict['item'] = statistics_val

    # Migrate item versions
    # Get connections to database - the versions data are directly added into the database
    c5_dspace = connect_to_db(database=const.CLARIN_DSPACE_NAME,
                                   host=const.CLARIN_DSPACE_HOST,
                                   user=const.CLARIN_DSPACE_USER,
                                   password=const.CLARIN_DSPACE_PASSWORD)
    c7_dspace = connect_to_db(database=const.CLARIN_DSPACE_7_NAME,
                              host=const.CLARIN_DSPACE_7_HOST,
                              port=const.CLARIN_DSPACE_7_PORT,
                              user=const.CLARIN_DSPACE_7_USER,
                              password=const.CLARIN_DSPACE_7_PASSWORD)
    # Store Items for which the version was created in some list - do not create a version for the same item
    processed_items_id = []
    # Some item versions cannot be imported into the database because they are already withdrawn and a new versions
    # are stored in another repository
    withdrawn_item_handles = []
    # Handle Item versions which cannot be imported because of some error
    not_imported_item_handles = []
    # Migration process
    migrate_item_history(metadata_class, items_dict, item_id_dict, item_handle_item_metadata_dict, c7_dspace,
                         processed_items_id, withdrawn_item_handles, not_imported_item_handles)

    # Check if migration was successful - if not log unsuccessful items
    check_sum(c7_dspace, c5_dspace, item_id_dict, withdrawn_item_handles, not_imported_item_handles,
              item_handle_item_metadata_dict)

    # Add result of version importing into statistics
    statistics_dict['versions_imported'] = (-1, len(processed_items_id))
    statistics_dict['versions_not_imported_withdrawn'] = (-1, len(withdrawn_item_handles))
    statistics_dict['versions_not_imported_error'] = (-1, len(not_imported_item_handles))
    logging.info("Item and Collection2item were successfully imported!")


def check_sum(c7_dspace, c5_dspace, item_id_dict, withdrawn_item_handles, not_imported_item_handles,
              item_handle_item_metadata_dict):
    """
    Check if item versions importing was successful
    Select item ids from CLARIN-DSpace5 which has some version metadata
    Select items uuids from CLARIN-DSpace7 `versionitem` table where are stored item's version
    Check if all items from CLARIN-DSpace5 has record in the CLARIN-DSpace7 history version table - check uuids
    """

    cursor_5 = c5_dspace.cursor()
    cursor_7 = c7_dspace.cursor()
    # Select item ids from CLARIN-DSpace5 which has some version metadata
    cursor_5.execute("SELECT resource_id FROM metadatavalue WHERE metadata_field_id in (50,51) group by resource_id;")
    # Fetch the result
    clarin_5_item_ids = cursor_5.fetchall()

    # Select item uuids from CLARIN-DSpace7 which record in the `versionitem` table
    cursor_7.execute("select item_id from versionitem;")
    # Fetch the result
    clarin_7_item_uuids = cursor_7.fetchall()

    if clarin_5_item_ids is None or clarin_7_item_uuids is None:
        logging.error('Cannot check result of importing item versions.')

    # Some new version of the item is not finished yet - item_id
    worklfowitem_not_imported = []
    # Some items could not be imported - uuid
    not_imported_items = []
    clarin_5_ids_to_uuid = []
    # Convert item_id to uuid
    for clarin_5_id in clarin_5_item_ids:
        clarin_5_ids_to_uuid.append(item_id_dict[clarin_5_id[0]])

    # Check if the clarin_5_uuid is in the clarin_7_historyversion_uuid
    for clarin_7_uuid in clarin_7_item_uuids:
        # clarin_5_uuid = item_id_dict[clarin_5_id]
        if clarin_7_uuid[0] not in clarin_5_ids_to_uuid:
            not_imported_items.append(clarin_7_uuid[0])

    if not_imported_items:
        logging.warning('Version migration MAYBE was not successful for the items below because the item could be'
                        ' a workspace or previous version is withdrawn.')
        for non_imported_uuid in not_imported_items:
            logging.warning(f'Please check versions for the Item with: {non_imported_uuid}')
        return
    logging.info('Version migration was successful.')


def migrate_item_history(metadata_class,
                         items_dict,
                         item_id_dict,
                         item_handle_item_metadata_dict,
                         c7_dspace,
                         processed_items_id,
                         withdrawn_item_handles,
                         not_imported_item_handles):
    logging.info("Going to migrate versions of all items.")

    cursor_c7_dspace = c7_dspace.cursor()
    admin_uuid = get_admin_uuid(cursor_c7_dspace)

    # 1. Create sequence of item versions
    #

    for item in items_dict.values():
        item_id = item['item_id']
        # Do not process versions of the item that have already been processed.
        if item_id in processed_items_id:
            continue

        # This sequence contains handles of all versions of the Item ordered from the first version to the latest one
        item_version_sequence = get_item_version_sequence(item_id, items_dict, metadata_class,
                                                          item_handle_item_metadata_dict, withdrawn_item_handles,
                                                          not_imported_item_handles)

        # Do not process item which does not have any version
        if item_version_sequence is None:
            continue

        logging.debug(f'Going to process all versions for the item with ID: {item_id}')
        # All versions of this Item is going to be processed
        # Insert data into `versionhistory` table
        versionhistory_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionhistory', 'versionhistory_id') + 1
        cursor_c7_dspace.execute("INSERT INTO versionhistory(versionhistory_id) VALUES (" +
                                 str(versionhistory_new_id) + ");")
        # Update sequence
        cursor_c7_dspace.execute(f"SELECT setval('versionhistory_seq', {versionhistory_new_id})")
        c7_dspace.commit()

        # Insert data into `versionitem` with `versionhistory` id
        versionitem_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionitem', 'versionitem_id') + 1
        for index, item_version_handle in enumerate(item_version_sequence, 1):
            # If the item is withdrawn the new version could be stored in our repo or in another. Do import the version
            # only if the item is stored in our repo.
            if item_version_handle not in item_handle_item_metadata_dict:
                current_item = items_dict[item_id]
                if current_item['withdrawn']:
                    logging.info(f'The item with handle: {item_version_handle} cannot be migrated because'
                                 f' it is stored in another repository.')
                    continue

            # Get the handle of the x.th version of the Item
            item_handle_id_dict = item_handle_item_metadata_dict[item_version_handle]
            # Get item_id using the handle
            item_id = item_handle_id_dict['item_id']
            # Get the uuid of the item using the item_id
            item_uuid = item_id_dict[item_id]
            # timestamp is required column in the database
            timestamp = datetime.datetime.now()
            cursor_c7_dspace.execute(f'INSERT INTO public.versionitem(versionitem_id, version_number, version_date, version_summary, versionhistory_id, eperson_id, item_id) VALUES ('
                                     f'{versionitem_new_id}, '
                                     f'{index}, '
                                     f'\'{timestamp}\', '
                                     f'\'\', '
                                     f'{versionhistory_new_id}, '
                                     f'\'{admin_uuid}\', '
                                     f'\'{item_uuid}\');')
            # Update sequence
            cursor_c7_dspace.execute(f"SELECT setval('versionitem_seq', {versionitem_new_id})")
            versionitem_new_id += 1
            processed_items_id.append(item_id)
        c7_dspace.commit()


def get_admin_uuid(cursor):
    """
    Get uuid of the admin user
    """
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
    """
    Get id of the last record from the specific table
    @return: id of the last record
    """
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
        logging.info("No records in the table.")

    # Close the cursor and the database connection
    return last_id


def get_item_version_sequence(item_id,
                              items_dict,
                              metadata_class,
                              item_handle_item_metadata_dict,
                              withdrawn_item_handles,
                              not_imported_item_handles):
    """
    Return all versions of the item in ordered list from the first version to the latest including the handle of the
    current Item
    @return: list of the item versions or if the item doesn't have any version return None
    """
    # The newer versions of the item
    newer_versions = get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, True,
                                       withdrawn_item_handles, not_imported_item_handles)
    # The previous versions of the item
    previous_versions = get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, False,
                                          withdrawn_item_handles, not_imported_item_handles)
    # Previous versions are in wrong order - reverse the list
    previous_versions = previous_versions[::-1]

    # If this item does not have any version return a None
    if len(newer_versions) == 0 and len(previous_versions) == 0:
        return None

    # Get handle of the current Item
    current_item_handle = getFirstMetadataValue(item_id, metadata_class, DC_IDENTIFIER_URI_ID)
    if current_item_handle is None:
        logging.error(f'Cannot find handle for the item with id: {item_id}')
        not_imported_item_handles.append(item_id)
        return None

    return previous_versions + [current_item_handle] + newer_versions


def get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, previous_or_newer: bool,
                      withdrawn_item_handles, not_imported_item_handles):
    """
    Return all previous or newer versions of the item using connection between `dc.relation.replaces` and
    `dc.relation.isreplacedby` item metadata.
    @return: list of versions or empty list
    """
    # Get previous version - fetch metadata value from `dc.relation.replaces`
    # Get newer version - fetch metadata value from `dc.relation.isreplaced.by`
    metadata_field = DC_RELATION_REPLACES_ID
    if previous_or_newer:
        metadata_field = DC_RELATION_ISREPLACEDBY_ID

    list_of_version = []
    current_item_id = item_id
    # current_version is handle of previous or newer item
    current_version = getFirstMetadataValue(current_item_id, metadata_class, metadata_field)
    while current_version is not None:
        if current_version not in item_handle_item_metadata_dict:
            # Check if current item is withdrawn
            item = items_dict[item_id]
            if item['withdrawn']:
                # The item is withdrawn and stored in another repository
                logging.info(f'The item with handle: {current_version} is withdrawn and will not be imported because '
                             f'it is stored in another repository.')
                withdrawn_item_handles.append(current_version)
            else:
                logging.error(f'The item with handle: {current_version} has not been imported!')
                not_imported_item_handles.append(current_version)
            current_version = None
            continue

        list_of_version.append(current_version)

        current_item_id = item_handle_item_metadata_dict[current_version]['item_id']
        current_version = getFirstMetadataValue(current_item_id, metadata_class, metadata_field)

    return list_of_version


def getFirstMetadataValue(item_id, metadata_class, metadata_field_id):
    if item_id is None:
        return None

    # 2 = resource_type = Item
    try:
        # It returns a dict of metadata_values
        all_metadata_values = metadata_class.metadatavalue_dict[(2, item_id)]
        # because metadata value are stored in the list
        for metadata_value in all_metadata_values:
            if metadata_value['metadata_field_id'] != metadata_field_id:
                continue
            # Return first value
            return metadata_value['text_value']
        # if metadata_field_id not in all_metadata_values:
        #     return None
    except Exception as e:
        logging.error(f'Cannot get first metadata from the Item with ID: {item_id} because: {e}')
    return None


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

