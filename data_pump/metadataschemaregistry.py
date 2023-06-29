import logging

from utils import read_json, convert_response_to_json, do_api_get_all, do_api_post


def import_metadataschemaregistry(metadata_schema_id, statistics):
    """
    Import data into database.
    Mapped tables: metadataschemaregistry
    """
    json_name = 'metadataschemaregistry.json'
    url_get_all = 'core/metadataschemas'
    url = 'core/metadataschemas'
    imported = 0
    # get all existing data from database table
    try:
        response = do_api_get_all(url_get_all)
        existing_data = convert_response_to_json(response)['_embedded']['metadataschemas']
    except Exception:
        logging.error('GET request ' + response.url + ' failed.')

    json_a = read_json(json_name)
    if not json_a:
        logging.info("Metadataschemaregistry JSON is empty.")
        return
    for i in json_a:
        json_p = {'namespace': i['namespace'], 'prefix': i['short_id']}
        # prefix has to be unique
        try:
            response = do_api_post(url, None, json_p)
            metadata_schema_id[i['metadata_schema_id']] = convert_response_to_json(response)[
                'id']
            imported += 1
        except Exception:
            found = False
            if not existing_data:
                logging.error('POST request ' + response.url + ' for id: ' + str(
                    i['metadata_schema_id']) + ' failed. Status: ' + str(response.status_code))
                continue
            for j in existing_data:
                if j['prefix'] != i['short_id']:
                    continue
                metadata_schema_id[i['metadata_schema_id']] = j['id']
                logging.info('Metadataschemaregistry '
                             ' prefix: ' + i['short_id']
                             + 'already exists in database!')
                found = True
                imported += 1
                break
            if not found:
                logging.error('POST request ' + response.url + ' for id: ' + str(
                    i['metadata_schema_id']) + ' failed. Status: ' + str(response.status_code))

    statistics['metadataschemaregistry'] = (len(json_a), imported)
    logging.info("MetadataSchemaRegistry was successfully imported!")
