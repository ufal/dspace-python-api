import logging
import json

from utils import read_json, convert_response_to_json, do_api_post


def import_bitstream(metadata, bitstreamformat_id, primaryBitstream, bitstream2bundle, bundle_id,
                     community2logo, collection2logo, bitstream_id, community_id, collection_id, unknown_format_id,
                     metadatavalue, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: bitstream, bundle2bitstream, metadata, most_recent_checksum and checksum_result
    """
    json_name = 'bitstream.json'
    url_checksum = 'clarin/import/core/bitstream/checksum'
    url = 'clarin/import/core/bitstream'
    imported = 0
    # load bundle2bitstream
    json_a = read_json("bundle2bitstream.json")
    if json_a:
        for i in json_a:
            bitstream2bundle[i['bitstream_id']] = i['bundle_id']

    # load and import bitstreams
    json_a = read_json(json_name)
    if not json_a:
        logging.info("Bitstream JSON is empty.")
        return
    counter = 0
    for i in json_a:
        if counter % 500 == 0:
            # do bitstream checksum
            # fill the tables: most_recent_checksum and checksum_result based on imported bitstreams
            try:
                do_api_post(url_checksum, None, None)
            except Exception as e:
                json_e = json.loads(e.args[0])
                logging.error('POST request ' +
                              json_e['path'] + ' failed. Status: ' + str(json_e['status']))
            counter = 0
        counter += 1
        json_p = dict()
        metadata_bitstream = metadata.get_metadata_value(
            metadatavalue, metadata_field_id, 0, i['bitstream_id'])
        if metadata_bitstream:
            json_p['metadata'] = metadata_bitstream
            # i['size_bytes']
        json_p['sizeBytes'] = 1748
        # i['checksum']
        json_p['checkSum'] = {'checkSumAlgorithm': i['checksum_algorithm'],
                              'value': '8a4605be74aa9ea9d79846c1fba20a33'}
        if not i['bitstream_format_id']:
            logging.info(
                f'Bitstream {i["bitstream_id"]} does not have a bitstream_format_id. Using {unknown_format_id} instead.')
            i['bitstream_format_id'] = unknown_format_id
            # i['internal_id']
        params = {'internal_id': '77893754617268908529226218097860272513',
                  'storeNumber': i['store_number'],
                  'bitstreamFormat': bitstreamformat_id[i['bitstream_format_id']],
                  'deleted': i['deleted'],
                  'sequenceId': i['sequence_id'],
                  'bundle_id': None,
                  'primaryBundle_id': None}

        # if bitstream has bundle, set bundle_id from None to id
        if i['bitstream_id'] in bitstream2bundle:
            params['bundle_id'] = bundle_id[bitstream2bundle[i['bitstream_id']]]

        # if bitstream is primary bitstream of some bundle, set primaryBundle_id from None to id
        if i['bitstream_id'] in primaryBitstream:
            params['primaryBundle_id'] = bundle_id[primaryBitstream[i['bitstream_id']]]
        try:
            logging.info('Going to process Bitstream with internal_id: ' +
                         str(i['internal_id']))
            response = do_api_post(url, params, json_p)
            bitstream_id[i['bitstream_id']] = convert_response_to_json(response)['id']
            imported += 1
        except Exception:
            logging.error(
                'POST request ' + response.url + ' for id: ' + str(i['bitstream_id']) + ' failed. Status: ' +
                str(response.status_code))

    statistics['bitstream'] = (len(json_a), imported)
    # add logos (bitstreams) to collections and communities
    add_logo_to_community(community2logo, bitstream_id, community_id)
    add_logo_to_collection(collection2logo, bitstream_id, collection_id)

    logging.info(
        "Bitstream, bundle2bitstream, most_recent_checksum and checksum_result were successfully imported!")


def add_logo_to_community(community2logo, bitstream_id, community_id):
    """
    Add bitstream to community as community logo.
    Logo has to exist in database.
    """
    if not community2logo:
        logging.info("There are no logos for communities.")
        return
    for key, value in community2logo.items():
        if key not in community_id or value not in bitstream_id:
            continue
        params = {'community_id': community_id[key], 'bitstream_id': bitstream_id[value]}
        try:
            response = do_api_post("clarin/import/logo/community", params, None)
        except Exception:
            logging.error('POST request ' + response.url +
                          ' failed. Status: ' + str(response.status_code))


def add_logo_to_collection(collection2logo, bitstream_id, collection_id):
    """
    Add bitstream to collection as collection logo.
    Logo has to exist in database.
    """
    if not collection2logo:
        logging.info("There are no logos for collections.")
        return
    for key, value in collection2logo.items():
        if key not in collection_id or value not in bitstream_id:
            continue
        params = {'collection_id': collection_id[key],
                  'bitstream_id': bitstream_id[value]}
        try:
            response = do_api_post("clarin/import/logo/collection", params, None)
        except Exception:
            logging.error('POST request ' + response.url +
                          ' failed. Status: ' + str(response.status_code))
