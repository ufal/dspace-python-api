import logging
import json

from utils import read_json, convert_response_to_json, do_api_post


def import_bitstream(metadata_class,
                     bitstreamformat_id_dict,
                     primary_bitstream_dict,
                     bitstream2bundle_dict,
                     bundle_id_dict,
                     community2logo_dict,
                     collection2logo_dict,
                     bitstream_id_dict,
                     community_id_dict,
                     collection_id_dict,
                     unknown_format_id_val,
                     statistics_dict):
    """
    Import data into database.
    Mapped tables: bitstream, bundle2bitstream, metadata, most_recent_checksum
    and checksum_result
    """
    bitstream_json_name = 'bitstream.json'
    bundle2bitstream_json_name = 'bundle2bitstream.json'
    bitstream_url = 'clarin/import/core/bitstream'
    checksum_url = 'clarin/import/core/bitstream/checksum'
    imported = 0

    # load bundle2bitstream
    bundle2bitstream_json_a = read_json(bundle2bitstream_json_name)
    if bundle2bitstream_json_a:
        for bundle2bitstream in bundle2bitstream_json_a:
            bitstream2bundle_dict[bundle2bitstream['bitstream_id']] = \
                bundle2bitstream['bundle_id']

    # load and import bitstreams
    bitstream_json_a = read_json(bitstream_json_name)
    if not bitstream_json_a:
        logging.info("Bitstream JSON is empty.")
        return
    counter = 0
    for bitstream in bitstream_json_a:
        if counter % 500 == 0:
            # do bitstream checksum
            # do this after every 500 imported bitstreams,
            # because the server may be out of memory
            # fill the tables: most_recent_checksum and checksum_result
            # based on imported bitstreams
            try:
                do_api_post(checksum_url, {}, None)
            except Exception as e:
                e_json = json.loads(e.args[0])
                logging.error('POST request ' +
                              e_json['path'] + ' failed. Exception: ' + str(e))
            counter = 0
        counter += 1
        bitstream_json_p = {}
        metadata_bitstream_dict = \
            metadata_class.get_metadata_value(0, bitstream['bitstream_id'])
        if metadata_bitstream_dict is not None:
            bitstream_json_p['metadata'] = metadata_bitstream_dict
            # bitstream['size_bytes']
        bitstream_json_p['sizeBytes'] = 1748
        # bitstream['checksum']
        bitstream_json_p['checkSum'] = {
            'checkSumAlgorithm': bitstream['checksum_algorithm'],
            'value': '8a4605be74aa9ea9d79846c1fba20a33'
        }
        if not bitstream['bitstream_format_id']:
            logging.info(
                f'Bitstream {bitstream["bitstream_id"]} '
                f'does not have a bitstream_format_id. '
                f'Using {unknown_format_id_val} instead.')
            bitstream['bitstream_format_id'] = unknown_format_id_val
            # bitstream['internal_id']
        params = {'internal_id': '77893754617268908529226218097860272513',
                  'storeNumber': bitstream['store_number'],
                  'bitstreamFormat': bitstreamformat_id_dict[
                      bitstream['bitstream_format_id']],
                  'deleted': bitstream['deleted'],
                  'sequenceId': bitstream['sequence_id'],
                  'bundle_id': None,
                  'primaryBundle_id': None}

        # if bitstream has bundle, set bundle_id from None to id
        if bitstream['bitstream_id'] in bitstream2bundle_dict:
            params['bundle_id'] = \
                bundle_id_dict[bitstream2bundle_dict[bitstream['bitstream_id']]]

        # if bitstream is primary bitstream of some bundle,
        # set primaryBundle_id from None to id
        if bitstream['bitstream_id'] in primary_bitstream_dict:
            params['primaryBundle_id'] = \
                bundle_id_dict[primary_bitstream_dict[bitstream['bitstream_id']]]
        try:
            logging.info('Going to process Bitstream with internal_id: ' +
                         str(bitstream['internal_id']))
            response = do_api_post(bitstream_url, params, bitstream_json_p)
            bitstream_id_dict[bitstream['bitstream_id']] = \
                convert_response_to_json(response)['id']
            imported += 1
        except Exception as e:
            logging.error(
                'POST request ' + bitstream_url + ' for id: ' +
                str(bitstream['bitstream_id']) + ' failed. Exception: ' +
                str(e))

    statistics_val = (len(bitstream_json_a), imported)
    statistics_dict['bitstream'] = statistics_val
    # add logos (bitstreams) to collections and communities
    add_logo_to_community(community2logo_dict, bitstream_id_dict, community_id_dict)
    add_logo_to_collection(collection2logo_dict, bitstream_id_dict, collection_id_dict)

    logging.info(
        "Bitstream, bundle2bitstream, most_recent_checksum "
        "and checksum_result were successfully imported!")


def add_logo_to_community(community2logo_dict, bitstream_id_dict, community_id_dict):
    """
    Add bitstream to community as community logo.
    Logo has to exist in database.
    """
    logo_comm_url = 'clarin/import/logo/community'
    if not community2logo_dict:
        logging.info("There are no logos for communities.")
        return
    for key, value in community2logo_dict.items():
        if key not in community_id_dict or value not in bitstream_id_dict:
            continue
        params = {
            'community_id': community_id_dict[key],
            'bitstream_id': bitstream_id_dict[value]
        }
        try:
            do_api_post(logo_comm_url, params, None)
        except Exception as e:
            logging.error('POST request ' + logo_comm_url +
                          ' failed. Exception: ' + str(e))
    logging.info(
        "Logos for communities were successfully added!")


def add_logo_to_collection(collection2logo_dict, bitstream_id_dict, collection_id_dict):
    """
    Add bitstream to collection as collection logo.
    Logo has to exist in database.
    """
    logo_coll_url = 'clarin/import/logo/collection'
    if not collection2logo_dict:
        logging.info("There are no logos for collections.")
        return
    for key, value in collection2logo_dict.items():
        if key not in collection_id_dict or value not in bitstream_id_dict:
            continue
        params = {'collection_id': collection_id_dict[key],
                  'bitstream_id': bitstream_id_dict[value]}
        try:
            do_api_post(logo_coll_url, params, None)
        except Exception as e:
            logging.error('POST request ' + logo_coll_url +
                          ' failed. Exception: ' + str(e))
    logging.info(
        "Logos for collections were successfully added!")
