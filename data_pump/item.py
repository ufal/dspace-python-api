import logging

from utils import read_json, convert_response_to_json, do_api_post
from support.dspace_proxy import rest_proxy
from const import API_URL


def import_item(metadata, workflowitem_id, workspaceitem_id, item_id, collection_id,
                eperson_id, imported_handle, handle, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: item, collection2item,workspaceitem, cwf_workflowitem, metadata, handle
    """
    json_name_item = "item.json"
    url_item = 'clarin/import/item'
    json_name_workspace = "workspaceitem.json"
    json_name_workflow = 'workflowitem.json'
    url_workflow = 'clarin/import/workflowitem'
    imported = 0
    importedItem = 0
    # create dict from items by item id
    json_a = read_json(json_name_item)
    items = dict()
    if not json_a:
        logging.info("Item JSON is empty.")
        return
    for i in json_a:
        items[i['item_id']] = i
    statistics['item'] = (len(json_a), 0)

    # create item and workspaceitem
    json_a = read_json(json_name_workspace)
    if json_a:
        for i in json_a:
            item = items[i['item_id']]
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'],
                                 i['multiple_files'], i['stage_reached'], i['page_reached'], metadata,
                                 workspaceitem_id, item_id, collection_id, eperson_id, imported_handle, handle,
                                 metadatavalue, metadata_field_id)
            imported += 1
            del items[i['item_id']]

        statistics['workspaceitem'] = (len(json_a), imported)
        importedItem += imported
        logging.info("Workspaceitem was successfully imported!")
    else:
        logging.info("Workspaceitem JSON is empty.")
    # create workflowitem
    # workflowitem is created from workspaceitem
    # -1, because the workflowitem doesn't contain this attribute
    imported = 0
    json_a = read_json(json_name_workflow)
    if json_a:
        for i in json_a:
            item = items[i['item_id']]
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'],
                                 i['multiple_files'], -1, -
                                 1, metadata, workspaceitem_id, item_id, collection_id,
                                 eperson_id, imported_handle, handle, metadatavalue, metadata_field_id)
            # create workflowitem from created workspaceitem
            params = {'id': str(workspaceitem_id[i['item_id']])}
            try:
                response = do_api_post(url_workflow, params, None)
                workflowitem_id[i['workflow_id']] = response.headers['workflowitem_id']
                imported += 1
            except Exception:
                logging.error('POST request ' + response.url + ' for id: ' + str(i['item_id']) + ' failed. Status: '
                              + str(response.status_code))
            del items[i['item_id']]

        statistics['workflowitem'] = (len(json_a), imported)
        importedItem += imported
        logging.info("Cwf_workflowitem was successfully imported!")
    else:
        logging.info("Workflowitem JSON is empty.")

    # create other items
    for i in items.values():
        json_p = {'discoverable': i['discoverable'], 'inArchive': i['in_archive'],
                  'lastModified': i['last_modified'], 'withdrawn': i['withdrawn']}
        metadata_item = metadata.get_metadata_value(
            metadatavalue, metadata_field_id, 2, i['item_id'])
        if metadata_item:
            json_p['metadata'] = metadata_item
        if (2, i['item_id']) in handle:
            json_p['handle'] = handle[(2, i['item_id'])][0]['handle']
            imported_handle += 1
        params = {'owningCollection': collection_id[i['owning_collection']],
                  'epersonUUID': eperson_id[i['submitter_id']]}
        try:
            response = do_api_post(url_item, params, json_p)
            response_json = convert_response_to_json(response)
            item_id[i['item_id']] = response_json['id']
            importedItem += 1
        except Exception:
            logging.error('POST request ' + response.url + ' for id: ' + str(i['item_id']) + ' failed. Status: ' +
                          str(response.status_code))

    statistics['item'] = (statistics['item'][0], importedItem)
    logging.info("Item and Collection2item were successfully imported!")


def import_workspaceitem(item, owningCollectin, multipleTitles, publishedBefore, multipleFiles, stagereached,
                         pageReached, metadata, workspaceitem_id, item_id, collection_id,
                         eperson_id, imported_handle, handle, metadatavalue, metadata_field_id):
    """
    Auxiliary method for import item.
    Import data into database.
    Mapped tables: workspaceitem, metadata, handle
    """
    url_workspace = 'clarin/import/workspaceitem'
    json_p = {'discoverable': item['discoverable'], 'inArchive': item['in_archive'],
              'lastModified': item['last_modified'], 'withdrawn': item['withdrawn']}
    metadata_item = metadata.get_metadata_value(
        metadatavalue, metadata_field_id, 2, item['item_id'])
    if metadata_item:
        json_p['metadata'] = metadata_item
    if (2, item['item_id']) in handle:
        json_p['handle'] = handle[(2, item['item_id'])][0]['handle']
        imported_handle += 1
    # the params are workspaceitem attributes
    params = {'owningCollection': collection_id[owningCollectin],
              'multipleTitles': multipleTitles,
              'publishedBefore': publishedBefore,
              'multipleFiles': multipleFiles, 'stageReached': stagereached,
              'pageReached': pageReached,
              'epersonUUID': eperson_id[item['submitter_id']]}
    try:
        response = do_api_post(url_workspace, params, json_p)
        id = convert_response_to_json(response)['id']
        workspaceitem_id[item['item_id']] = id
        try:
            response = rest_proxy.d.api_get(
                API_URL + 'clarin/import/' + str(id) + "/item", None, None)
            item_id[item['item_id']] = convert_response_to_json(response)['id']
        except Exception:
            logging.error('POST request ' + response.url +
                          ' failed. Status: ' + str(response.status_code))
    except Exception:
        logging.error('POST request ' + response.url + ' for id: ' + str(item['item_id']) +
                      ' failed. Status: ' + str(response.status_code))
