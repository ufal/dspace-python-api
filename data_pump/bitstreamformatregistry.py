import logging

from utils import read_json, convert_response_to_json, do_api_get_all, do_api_post


def import_bitstreamformatregistry(bitstreamformat_id_dict, unknown_format_id_val,
                                   statistics_dict):
    """
    Import data into database.
    Mapped tables: bitstreamformatregistry
    """
    bitsteamformat_json_name = 'bitstreamformatregistry.json'
    bitstreamformat_url = 'core/bitstreamformats'
    imported = 0
    # read all existing data from bitstreamformatregistry
    shortDesc2Id_dict = dict()
    try:
        response = do_api_get_all(bitstreamformat_url)
        bitstreamformat_json = \
            convert_response_to_json(response)['_embedded']['bitstreamformats']
        if bitstreamformat_json:
            for i in bitstreamformat_json:
                shortDesc2Id_dict[i['shortDescription']] = i['id']
                if i['description'] == 'Unknown data format':
                    i['id']

        bitstreamformat_json_a = read_json(bitsteamformat_json_name)
        if not bitstreamformat_json_a:
            logging.info("Bitstreamformatregistry JSON is empty.")
            return
        for i in bitstreamformat_json_a:
            level = i['support_level']
            if level == 0:
                level_str = "UNKNOWN"
            elif level == 1:
                level_str = "KNOWN"
            elif level == 2:
                level_str = "SUPPORTED"
            else:
                logging.error('Unsupported bitstream format registry id: ' + str(level))
                continue

            bitstreamformat_json_p = {'mimetype': i['mimetype'],
                                      'description': i['description'],
                                      'shortDescription': i['short_description'],
                                      'supportLevel': level_str,
                                      'internal': i['internal']}
            try:
                response = do_api_post(bitstreamformat_url, None,
                                       bitstreamformat_json_p)
                bitstreamformat_id_dict[i['bitstream_format_id']] = \
                    convert_response_to_json(response)['id']
                imported += 1
            except Exception:
                if response.status_code == 200 or response.status_code == 201:
                    bitstreamformat_id_dict[i['bitstream_format_id']] = \
                        shortDesc2Id_dict[i['short_description']]
                    logging.info('Bitstreamformatregistry with short description ' +
                                 i['short_description'] +
                                 ' already exists in database!')
                else:
                    logging.error('POST request ' + response.url + ' for id: ' +
                                  str(i['bitstream_format_id']) +
                                  ' failed. Status: ' + str(response.status_code))
        statistics_val = (len(bitstreamformat_json_a), imported)
        statistics_dict['bitstreamformatregistry'] = statistics_val
    except Exception:
        logging.error('GET request ' + response.url +
                      ' failed. Status: ' + str(response.status_code))

    logging.info("Bitstream format registry was successfully imported!")
