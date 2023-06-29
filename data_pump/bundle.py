import logging

from utils import read_json, convert_response_to_json, do_api_post


def import_bundle(metadata, item_id, bundle_id, primaryBitstream, metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: item2bundle, bundle
    """
    json_name_item2bundle = 'item2bundle.json'
    json_name_bundle = 'bundle.json'
    url = 'core/items/'
    imported = 0
    # load item2bundle into dict
    json_a = read_json(json_name_item2bundle)
    statistics['item2bundle'] = (len(json_a), 0)
    item2bundle = dict()
    if not json_a:
        logging.info("Item2bundle JSON is empty.")
        return
    for i in json_a:
        if i['item_id'] in item2bundle:
            item2bundle[i['item_id']].append(i['bundle_id'])
        else:
            item2bundle[i['item_id']] = [i['bundle_id']]

    # load bundles and map bundles to their primary bitstream ids
    json_a = read_json(json_name_bundle)
    if not json_a:
        logging.info("Bundle JSON is empty.")
        return
    for i in json_a:
        if i['primary_bitstream_id']:
            primaryBitstream[i['primary_bitstream_id']] = i['bundle_id']

    # import bundle without primary bitstream id
    if not item2bundle:
        logging.info("Bundle JSON is empty.")
        return
    for item in item2bundle.items():
        for bundle in item[1]:
            json_p = dict()
            metadata_bundle = metadata.get_metadata_value(metadatavalue, metadata_field_id, 1, bundle)
            if metadata_bundle:
                json_p['metadata'] = metadata_bundle
                json_p['name'] = metadata_bundle['dc.title'][0]['value']

            try:
                response = do_api_post(url + str(item_id[item[0]]) + "/bundles", None, json_p)
                bundle_id[bundle] = convert_response_to_json(response)['uuid']
                imported += 1
            except Exception as e:
                logging.error('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

    statistics['item2bundle'] = (statistics['item2bundle'][0], imported)
    logging.info("Bundle and Item2Bundle were successfully imported!")
