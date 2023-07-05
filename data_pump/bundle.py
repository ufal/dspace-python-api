import logging

from utils import read_json, convert_response_to_json, do_api_post, save_dict_as_json


def import_bundle(metadata_class,
                  item_id_dict,
                  bundle_id_dict,
                  primary_bitstream_dict,
                  statistics_dict,
                  save_dict=False):
    """
    Import data into database.
    Mapped tables: item2bundle, bundle
    """
    item2bundle_json_name = 'item2bundle.json'
    bundle_json_name = 'bundle.json'
    item_url = 'core/items/'
    imported = 0
    # load item2bundle into dict
    item2bundle_json_a = read_json(item2bundle_json_name)
    statistics_val = (len(item2bundle_json_a), 0)
    statistics_dict['item2bundle'] = statistics_val
    item2bundle_dict = {}
    if not item2bundle_json_a:
        logging.info("Item2bundle JSON is empty.")
        return
    for item2bundle in item2bundle_json_a:
        if item2bundle['item_id'] in item2bundle_dict:
            item2bundle_dict[item2bundle['item_id']].append(item2bundle['bundle_id'])
        else:
            item2bundle_dict[item2bundle['item_id']] = [item2bundle['bundle_id']]

    # load bundles and map bundles to their primary bitstream ids
    bundle_json_a = read_json(bundle_json_name)
    if not bundle_json_a:
        logging.info("Bundle JSON is empty.")
        return
    for bundle in bundle_json_a:
        if bundle['primary_bitstream_id']:
            primary_bitstream_dict[bundle['primary_bitstream_id']] = bundle['bundle_id']

    # import bundle without primary bitstream id
    if not item2bundle_dict:
        logging.info("Bundle JSON is empty.")
        return
    for item in item2bundle_dict.items():
        for bundle in item[1]:
            bundle_json_p = {}
            metadata_bundle_dict = metadata_class.get_metadata_value(1, bundle)
            if metadata_bundle_dict:
                bundle_json_p['metadata'] = metadata_bundle_dict
                bundle_json_p['name'] = metadata_bundle_dict['dc.title'][0]['value']

            bundle_url = item_url
            try:
                bundle_url += str(item_id_dict[item[0]]) + "/bundles"
                response = do_api_post(bundle_url, {}, bundle_json_p)
                bundle_id_dict[bundle] = convert_response_to_json(response)['uuid']
                imported += 1
            except Exception as e:
                logging.error('POST request ' + bundle_url +
                              ' failed. Exception: ' + str(e))

    # save bundle dict as json
    if save_dict:
        save_dict_as_json(bundle_json_name, bundle_id_dict)
    statistics_val = (statistics_dict['item2bundle'][0], imported)
    statistics_dict['item2bundle'] = statistics_val
    logging.info("Bundle and Item2Bundle were successfully imported!")
