import logging

from data_pump.utils import read_json, convert_response_to_json, do_api_get_all, \
    do_api_post, save_dict_as_json


def import_bitstreamformatregistry(bitstreamformat_id_dict,
                                   unknown_format_id_val,
                                   statistics_dict,
                                   save_dict):
    """
    Import data into database.
    Mapped tables: bitstreamformatregistry
    """
    bitsteamformat_json_name = 'bitstreamformatregistry.json'
    saved_bitsteamformat_json_name = 'bitstreamformatregistry_dict.json'
    bitstreamformat_url = 'core/bitstreamformats'
    imported = 0
    # read all existing data from bitstreamformatregistry
    shortDesc2Id_dict = {}
    try:
        response = do_api_get_all(bitstreamformat_url)
        bitstreamformat_json = \
            convert_response_to_json(response)['_embedded']['bitstreamformats']
        if bitstreamformat_json is not None:
            for bitstreamformat in bitstreamformat_json:
                shortDesc2Id_dict[bitstreamformat['shortDescription']] = \
                    bitstreamformat['id']
                if bitstreamformat['description'] == 'Unknown data format':
                    unknown_format_id_val = bitstreamformat['id']

        bitstreamformat_json_list = read_json(bitsteamformat_json_name)
        if not bitstreamformat_json_list:
            logging.info("Bitstreamformatregistry JSON is empty.")
            return

        for bitstreamformat in bitstreamformat_json_list:
            level = bitstreamformat['support_level']
            if level == 0:
                level_str = "UNKNOWN"
            elif level == 1:
                level_str = "KNOWN"
            elif level == 2:
                level_str = "SUPPORTED"
            else:
                logging.error('Unsupported bitstream format registry id: ' + str(level))
                continue

            bitstreamformat_json_p = {
                'mimetype': bitstreamformat['mimetype'],
                'description': bitstreamformat['description'],
                'shortDescription': bitstreamformat['short_description'],
                'supportLevel': level_str,
                'internal': bitstreamformat['internal']
            }
            try:
                response = do_api_post(bitstreamformat_url, {},
                                       bitstreamformat_json_p)
                bitstreamformat_id_dict[bitstreamformat['bitstream_format_id']] = \
                    convert_response_to_json(response)['id']
                imported += 1
            except Exception as e:
                if response.status_code == 200 or response.status_code == 201:
                    bitstreamformat_id_dict[bitstreamformat['bitstream_format_id']] = \
                        shortDesc2Id_dict[bitstreamformat['short_description']]
                    logging.info('Bitstreamformatregistry with short description ' +
                                 bitstreamformat['short_description'] +
                                 ' already exists in database!')
                else:
                    logging.error('POST request ' + bitstreamformat_url + ' for id: ' +
                                  str(bitstreamformat['bitstream_format_id']) +
                                  ' failed. Exception: ' + str(e))

        # save bitstreamregistry dict as json
        if save_dict:
            save_dict_as_json(saved_bitsteamformat_json_name, bitstreamformat_id_dict)
        statistics_val = (len(bitstreamformat_json_list), imported)
        statistics_dict['bitstreamformatregistry'] = statistics_val
    except Exception as e:
        logging.error('GET request ' + bitstreamformat_url +
                      ' failed. Exception: ' + str(e))

    logging.info("Bitstream format registry was successfully imported!")
    return unknown_format_id_val
