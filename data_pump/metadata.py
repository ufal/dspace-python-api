import logging


from data_pump.utils import read_json, convert_response_to_json, \
    do_api_get_one, do_api_get_all, do_api_post, save_dict_as_json, \
    create_dict_from_json


class Metadata:
    def __init__(self, statistics_dict, load_dict):
        """
        Read metadatavalue as json and
        convert it to dictionary with tuple key: resource_type_id and resource_id.
        """
        self.metadatavalue_dict = {}
        self.metadataschema_id_dict = {}
        self.metadatafield_id_dict = {}
        if load_dict:
            self.metadataschema_id_dict = \
                create_dict_from_json("metadataschemaregistry.json")
            self.metadatafield_id_dict = \
                create_dict_from_json("metadatafieldregistry.json")

        # import all metadata
        self.read_metadata()
        self.import_metadataschemaregistry(statistics_dict)
        self.import_metadatafieldregistry(statistics_dict)

    def read_metadata(self):
        metadatavalue_json_name = 'metadatavalue.json'
        metadatafield_json_name = 'metadatafieldregistry.json'

        metadatavalue_json_list = read_json(metadatavalue_json_name)
        if not metadatavalue_json_list:
            logging.info('Metadatavalue JSON is empty.')
            return

        metadatafield_json_list = read_json(metadatafield_json_name)
        sponsor_field_id = -1
        if not metadatafield_json_list:
            logging.info('Metadatafield JSON is empty.')
            return

        # Find out which field is `local.sponsor`, check only `sponsor` string
        for metadatafield in metadatafield_json_list:
            element = metadatafield['element']
            if element != 'sponsor':
                continue
            sponsor_field_id = metadatafield['metadata_field_id']

        for metadatavalue in metadatavalue_json_list:
            key = (metadatavalue['resource_type_id'], metadatavalue['resource_id'])
            # replace separator @@ by ;
            metadatavalue['text_value'] = metadatavalue['text_value'].replace("@@", ";")
            # replace `local.sponsor` data sequence
            # from `<ORG>;<PROJECT_CODE>;<PROJECT_NAME>;<TYPE>`
            # to `<TYPE>;<PROJECT_CODE>;<ORG>;<PROJECT_NAME>`
            if metadatavalue['metadata_field_id'] == sponsor_field_id:
                metadatavalue['text_value'] = \
                    self.fix_local_sponsor_sequence(metadatavalue['text_value'])
            if key in self.metadatavalue_dict.keys():
                self.metadatavalue_dict[key].append(metadatavalue)
            else:
                self.metadatavalue_dict[key] = [metadatavalue]

    @staticmethod
    def fix_local_sponsor_sequence(wrong_sequence_str):
        """
        Replace `local.sponsor` data sequence
        from `<ORG>;<PROJECT_CODE>;<PROJECT_NAME>;<TYPE>;<EU_IDENTIFIER>`
        to `<TYPE>;<PROJECT_CODE>;<ORG>;<PROJECT_NAME>;<EU_IDENTIFIER>`
        """
        separator = ';'
        sponsor_list_max_length = 5

        # sponsor list could have length 4 or 5
        sponsor_list = wrong_sequence_str.split(separator)
        org = sponsor_list[0]
        project_code = sponsor_list[1]
        project_name = sponsor_list[2]
        project_type = sponsor_list[3]
        eu_identifier = ''
        if len(sponsor_list) == sponsor_list_max_length:
            # has eu_identifier value
            eu_identifier = sponsor_list[4]
        # compose the `local.sponsor` sequence in the right way
        return separator.join(
            [project_type, project_code, org, project_name, eu_identifier])

    def import_metadataschemaregistry(self, statistics_dict, save_dict=True):
        """
        Import data into database.
        Mapped tables: metadataschemaregistry
        """
        metadataschema_json_name = 'metadataschemaregistry.json'
        saved_metadataschema_json_name = 'metadataschema_dict.json'
        metadataschema_url = 'core/metadataschemas'
        imported = 0
        # get all existing data from database table
        existing_data_dict = Metadata.get_imported_metadataschemaregistry(
            metadataschema_url)

        metadataschema_json_list = read_json(metadataschema_json_name)
        if not metadataschema_json_list:
            logging.info("Metadataschemaregistry JSON is empty.")
            return
        for metadataschema in metadataschema_json_list:
            metadataschema_json_p = {
                'namespace': metadataschema['namespace'],
                'prefix': metadataschema['short_id']
            }
            # prefix has to be unique
            try:
                response = do_api_post(metadataschema_url, {}, metadataschema_json_p)
                self.metadataschema_id_dict[metadataschema['metadata_schema_id']] = \
                    convert_response_to_json(response)['id']
                imported += 1
            except Exception as e:
                found = False
                if not existing_data_dict:
                    logging.error('POST request ' + metadataschema_url + ' for id: ' +
                                  str(metadataschema['metadata_schema_id']) +
                                  ' failed. Exception: ' + str(e))
                    continue
                for existing_data in existing_data_dict:
                    if existing_data['prefix'] != metadataschema['short_id']:
                        continue
                    self.metadataschema_id_dict[metadataschema
                                                ['metadata_schema_id']] = \
                        existing_data['id']
                    logging.info('Metadataschemaregistry '
                                 ' prefix: ' + metadataschema['short_id']
                                 + 'already exists in database!')
                    found = True
                    imported += 1
                    break
                if not found:
                    logging.error('POST request ' + metadataschema_url + ' for id: ' +
                                  str(metadataschema['metadata_schema_id']) +
                                  ' failed. Exception: ' + str(e))

        # save metadataschema dict as json
        if save_dict:
            save_dict_as_json(saved_metadataschema_json_name,
                              self.metadataschema_id_dict)
        statistics_val = (len(metadataschema_json_list), imported)
        statistics_dict['metadataschemaregistry'] = statistics_val
        logging.info("MetadataSchemaRegistry was successfully imported!")

    @staticmethod
    def get_imported_metadataschemaregistry(metadataschema_url):
        """
        Gel all existing data from table metadataschemaregistry.
        """
        existing_data_dict = None
        try:
            response = do_api_get_all(metadataschema_url)
            existing_data_dict = convert_response_to_json(response)['_embedded'][
                'metadataschemas']
        except Exception as e:
            logging.error('GET request ' + metadataschema_url + ' failed. Exception: '
                          + str(e))
        return existing_data_dict

    def import_metadatafieldregistry(self, statistics_dict, save_dict=True):
        """
        Import data into database.
        Mapped tables: metadatafieldregistry
        """
        metadatafield_json_name = 'metadatafieldregistry.json'
        saved_metadatafield_json_name = 'metadatafield_dict.json'
        metadatafield_url = 'core/metadatafields'
        imported = 0
        existing_data_dict = None
        try:
            response = do_api_get_all(metadatafield_url)
            existing_data_dict = convert_response_to_json(response)['_embedded'][
                'metadatafields']
        except Exception as e:
            logging.error('GET request ' + metadatafield_url +
                          ' failed. Exception: ' + str(e))

        metadatafield_json_list = read_json(metadatafield_json_name)
        if not metadatafield_json_list:
            logging.info("Metadatafieldregistry JSON is empty.")
            return
        for metadatafield in metadatafield_json_list:
            metadatafield_json_p = {
                'element': metadatafield['element'],
                'qualifier': metadatafield['qualifier'],
                'scopeNote': metadatafield['scope_note']
            }
            params = {'schemaId': self.metadataschema_id_dict[
                metadatafield['metadata_schema_id']]}
            # element and qualifier have to be unique
            try:
                response = do_api_post(metadatafield_url, params, metadatafield_json_p)
                self.metadatafield_id_dict[metadatafield['metadata_field_id']] = \
                    convert_response_to_json(response)['id']
                imported += 1
            except Exception as e:
                found = False
                if not existing_data_dict:
                    logging.error('POST request ' + metadatafield_url + ' for id: ' +
                                  str(metadatafield['metadata_field_id']) +
                                  ' failed. Exception: ' + str(e))
                    continue
                for existing_data in existing_data_dict:
                    if existing_data['_embedded']['schema']['id'] != metadatafield['metadata_schema_id'] or \
                            existing_data['element'] != metadatafield['element'] or \
                            existing_data['qualifier'] != metadatafield['qualifier']:
                        continue
                    self.metadatafield_id_dict[metadatafield['metadata_field_id']] = \
                        existing_data['id']
                    logging.info('Metadatafieldregistry with element: ' +
                                 metadatafield['element'] +
                                 ' already exists in database!')
                    found = True
                    imported += 1
                    break
                if not found:
                    logging.error('POST request ' + metadatafield_url + ' for id: ' +
                                  str(metadatafield['metadata_field_id']) +
                                  ' failed. Exception: ' + str(e))

        # save metadatafield dict as json
        if save_dict:
            save_dict_as_json(saved_metadatafield_json_name, self.metadatafield_id_dict)
        statistics_val = (len(metadatafield_json_list), imported)
        statistics_dict['metadatafieldregistry'] = statistics_val
        logging.info("MetadataFieldRegistry was successfully imported!")

    def get_metadata_value(self, old_resource_type_id, old_resource_id):
        """
        Get metadata value for dspace object.
        """
        metadatafield_url = 'core/metadatafields'
        metadataschema_url = 'core/metadataschemas'
        result_dict = {}
        # get all metadatavalue for object
        if (old_resource_type_id, old_resource_id) not in self.metadatavalue_dict:
            logging.info('Metadatavalue for resource_type_id: ' +
                         str(old_resource_type_id) + ' and resource_id: ' +
                         str(old_resource_id) + 'does not exist.')
            return None
        metadatavalue_obj = self.metadatavalue_dict[(
            old_resource_type_id, old_resource_id)]
        # create list of object metadata
        for metadatavalue in metadatavalue_obj:
            if metadatavalue['metadata_field_id'] not in self.metadatafield_id_dict:
                continue
            try:
                response = do_api_get_one(
                    metadatafield_url,
                    self.metadatafield_id_dict[metadatavalue['metadata_field_id']])
                metadatafield_json = convert_response_to_json(response)
            except Exception as e:
                logging.error('GET request' + metadatafield_url +
                              ' failed. Exception: ' + str(e))
                continue
            # get metadataschema
            try:
                response = do_api_get_one(
                    metadataschema_url, metadatafield_json['_embedded']['schema']['id'])
                metadataschema_json = convert_response_to_json(response)
            except Exception as e:
                logging.error('GET request ' + metadataschema_url +
                              ' failed. Exception: ' + str(e))
                continue
            # define and insert key and value of dict
            key = metadataschema_json['prefix'] + '.' + metadatafield_json['element']
            value = {
                'value': metadatavalue['text_value'],
                'language': metadatavalue['text_lang'],
                'authority': metadatavalue['authority'],
                'confidence': metadatavalue['confidence'],
                'place': metadatavalue['place']
            }
            if metadatafield_json['qualifier']:
                key += '.' + metadatafield_json['qualifier']
            if key in result_dict.keys():
                result_dict[key].append(value)
            else:
                result_dict[key] = [value]
        return result_dict
