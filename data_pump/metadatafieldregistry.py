import logging

from utils import read_json, convert_response_to_json, do_api_get_all, do_api_post


def import_metadatafieldregistry(metadata_schema_id, metadata_field_id, statistics):
    """
    Import data into database.
    Mapped tables: metadatafieldregistry
    """
    json_name = 'metadatafieldregistry.json'
    url_get_all = 'core/metadatafields'
    url = 'core/metadatafields'
    imported = 0
    try:
        response = do_api_get_all(url_get_all)
        existing_data = convert_response_to_json(response)['_embedded']['metadatafields']
    except Exception as e:
        logging.error('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))

    json_a = read_json(json_name)
    if not json_a:
        logging.info("Metadatafieldregistry JSON is empty.")
        return
    for i in json_a:
        json_p = {'element': i['element'], 'qualifier': i['qualifier'], 'scopeNote': i['scope_note']}
        param = {'schemaId': metadata_schema_id[i['metadata_schema_id']]}
        # element and qualifier have to be unique
        try:
            response = do_api_post(url, param, json_p)
            metadata_field_id[i['metadata_field_id']] = convert_response_to_json(response)['id']
            imported += 1
        except Exception as e:
            found = False
            if not existing_data:
                logging.error('POST request ' + response.url + ' for id: ' + str(
                    i['metadata_field_id']) + ' failed. Status: ' + str(response.status_code))
                continue
            for j in existing_data:
                if j['element'] != i['element'] or j['qualifier'] != i['qualifier']:
                    continue
                metadata_field_id[i['metadata_field_id']] = j['id']
                logging.info('Metadatafieldregistry with element: ' + i['element'] + ' already exists in database!')
                found = True
                imported += 1
                break
            if not found:
                logging.error('POST request ' + response.url + ' for id: ' + str(
                    i['metadata_field_id']) + ' failed. Status: ' + str(response.status_code))

    statistics['metadatafieldregistry'] = (len(json_a), imported)
    logging.info("MetadataFieldRegistry was successfully imported!")
