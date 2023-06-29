import logging


from utils import read_json, convert_response_to_json, do_api_get_one

class Metadata:
    def __init__(self, metadatavalue):
        """
        Read metadatavalue as json and
        convert it to dictionary with tuple key: resource_type_id and resource_id.
        """
        json_name = 'metadatavalue.json'
        metadatavalue_json = read_json(json_name)
        if not metadatavalue_json:
            logging.info('Metadatavalue JSON is empty.')
            return
        for i in metadatavalue_json:
            key = (i['resource_type_id'], i['resource_id'])
            # replace separator @@ by ;
            i['text_value'] = i['text_value'].replace("@@", ";")
            if key in metadatavalue.keys():
                metadatavalue[key].append(i)
            else:
                metadatavalue[key] = [i]

    def get_metadata_value(self, metadatavalue, metadata_field_id, old_resource_type_id, old_resource_id):
        """
        Get metadata value for dspace object.
        """
        url_metadatafield = 'core/metadatafields'
        url_metadataschema = 'core/metadataschemas'
        result = dict()
        # get all metadatavalue for object
        if (old_resource_type_id, old_resource_id) not in metadatavalue:
            logging.info('Metadatavalue for resource_type_id: ' + str(old_resource_type_id) +
                         ' and resource_id: ' + str(old_resource_id) + 'does not exist.')
            return None
        metadatavalue_obj =metadatavalue[(old_resource_type_id, old_resource_id)]
        # create list of object metadata
        for i in metadatavalue_obj:
            if i['metadata_field_id'] not in metadata_field_id:
                continue
            try:
                response = do_api_get_one(url_metadatafield, metadata_field_id[i['metadata_field_id']])
                metadatafield_json = convert_response_to_json(response)
            except Exception as e:
                logging.error('GET request' + response.url + ' failed. Status: ' + str(response.status_code))
                continue
            # get metadataschema
            try:
                response = do_api_get_one(url_metadataschema, metadatafield_json['_embedded']['schema']['id'])
                metadataschema_json = convert_response_to_json(response)
            except Exception as e:
                logging.error('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))
                continue
            # define and insert key and value of dict
            key = metadataschema_json['prefix'] + '.' + metadatafield_json['element']
            value = {'value': i['text_value'], 'language': i['text_lang'], 'authority': i['authority'],
                     'confidence': i['confidence'], 'place': i['place']}
            if metadatafield_json['qualifier']:
                key += '.' + metadatafield_json['qualifier']
            if key in result.keys():
                result[key].append(value)
            else:
                result[key] = [value]
        return result
