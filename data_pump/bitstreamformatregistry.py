import logging

from utils import read_json, convert_response_to_json, do_api_get_all, do_api_post


def import_bitstreamformatregistry(bitstreamformat_id, unknown_format_id, statistics):
    """
    Import data into database.
    Mapped tables: bitstreamformatregistry
    """
    url_all = 'core/bitstreamformats'
    json_name = 'bitstreamformatregistry.json'
    url = 'core/bitstreamformats'
    imported = 0
    # read all existing data from bitstreamformatregistry
    shortDesc2Id = dict()
    try:
        response = do_api_get_all(url_all)
        bitstreamformat = convert_response_to_json(response)['_embedded']['bitstreamformats']
        if bitstreamformat:
            for i in bitstreamformat:
                shortDesc2Id[i['shortDescription']] = i['id']
                if i['description'] == 'Unknown data format':
                    unknown_format_id = i['id']

        json_a = read_json(json_name)
        if not json_a:
            logging.info("Bitstreamformatregistry JSON is empty.")
            return
        for i in json_a:
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

            json_p = {'mimetype': i['mimetype'], 'description': i['description'],
                      'shortDescription': i['short_description'], 'supportLevel': level_str,
                      'internal': i['internal']}
            try:
                response = do_api_post(url, None, json_p)
                bitstreamformat_id[i['bitstream_format_id']] = convert_response_to_json(response)['id']
                imported += 1
            except Exception as e:
                if response.status_code == 200 or response.status_code == 201:
                    bitstreamformat_id[i['bitstream_format_id']] = shortDesc2Id[i['short_description']]
                    logging.info('Bitstreamformatregistry with short description ' + i[
                        'short_description'] + ' already exists in database!')
                else:
                    logging.error('POST request ' + response.url + ' for id: ' + str(i['bitstream_format_id']) +
                                  ' failed. Status: ' + str(response.status_code))
        statistics['bitstreamformatregistry'] = (len(json_a), imported)
    except Exception as e:
        logging.error('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))

    logging.info("Bitstream format registry was successfully imported!")
