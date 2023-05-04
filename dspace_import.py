import json
import const
import migration_const

from support.logs import log
from support.dspace_proxy import rest_proxy

#global params
eperson_id = dict()
group_id = dict()
metadata_schema_id = dict()
metadata_field_id = dict()
community_id = dict()
community2logo = dict()
collection_id = dict()
collection2logo = dict()
item_id = dict()
workspaceitem_id = dict()
metadatavalue = dict()
handle = dict()
bitstreamformat_id = dict()
primaryBitstream = dict()
bitstream2bundle = dict()
bundle_id = dict()
bitstream_id = dict()

#functions
def read_json(file_name):
    """
    Read data from file as json.
    @param file_name: file name
    @return: data as json
    """
    x = open(migration_const.DATA_PATH + file_name)
    json_p = json.load(x)
    x.close()
    return json_p

def do_api_post(url, param, json_p):
    """
    Insert data into database by api if they are not None.
    @param url: url for api post
    @param param: parameters for api post
    @param json_p: posted data
    @return: response from api post
    """
    url = const.API_URL + url
    response = rest_proxy.d.api_post(url, param, json_p)
    return response

def do_api_get_one(url, id):
    """
    Get data with id from table.
    @param url: url for api get
    @param id: id of object
    @return: response from api get
    """
    url = const.API_URL + url + '/' + str(id)
    response = rest_proxy.d.api_get(url, None, None)
    return response

def do_api_get_all(url):
    """
    Get all data from table.
    @param url: url for api get
    @return: response from api get
    """
    url = const.API_URL + url
    param = {'size' : 1000}
    response = rest_proxy.d.api_get(url, param, None)
    return response

def convert_response_to_json(response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    return json.loads(response.content.decode('utf-8'))

def read_metadata():
    """
    Read metadatavalue as json and
    convert it to dictionary with tuple key: resource_type_id and resource_id.
    """
    global metadatavalue
    metadatavalue_json = read_json('metadatavalue.json')
    if metadatavalue_json:
        for i in metadatavalue_json:
            key = (i['resource_type_id'], i['resource_id'])
            #replace separator @@ by ;
            i['text_value'] = i['text_value'].replace("@@", ";")
            if key in metadatavalue.keys():
                metadatavalue[key].append(i)
            else:
                metadatavalue[key] = [i]

def read_handle():
    """
    Read handle as json and convert it to dictionary wth tuple key: resource_type_id and resource_type,
    where value is list of jsons.
    """
    global handle
    handle_json = read_json('handle.json')
    if handle_json:
        for i in handle_json:
            key = (i['resource_type_id'], i['resource_id'])
            if key in handle.keys():
                handle[key].append(i)
            else:
                handle[key] = [i]

def get_metadata_value(old_resource_type_id, old_resource_id):
    """
    Get metadata value for dspace object.
    """
    global metadata_field_id, metadatavalue
    result = dict()
    if not metadatavalue:
        read_metadata()
    #get all metadatavalue for object
    if (old_resource_type_id, old_resource_id) in metadatavalue:
        metadatavalue_obj = metadatavalue[(old_resource_type_id, old_resource_id)]
    else:
        return None
    if metadatavalue_obj:
        #create list of object metadata
        for i in metadatavalue_obj:
            if i['metadata_field_id'] in metadata_field_id:
                try:
                    response = do_api_get_one('core/metadatafields', metadata_field_id[i['metadata_field_id']])
                    metadatafield_json = convert_response_to_json(response)
                except:
                    log('GET request' + response.url + ' failed. Status: ' + str(response.status_code))
                    continue
                #get metadataschema
                try:
                    response = do_api_get_one('core/metadataschemas', metadatafield_json['_embedded']['schema']['id'])
                    metadataschema_json = convert_response_to_json(response)
                except:
                    log('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))
                    continue
                #define and insert key and value of dict
                key = metadataschema_json['prefix'] + '.' + metadatafield_json['element']
                value = {'value' : i['text_value'], 'language' : i['text_lang'], 'authority' : i['authority'],
                               'confidence' : i['confidence'], 'place' : i['place']}
                if metadatafield_json['qualifier']:
                    key += '.' + metadatafield_json['qualifier']
                if key in result.keys():
                    result[key].append(value)
                else:
                    result[key] = [value]
    return result

#table mapping
def import_licenses():
    """
    Import data into database.
    Mapped tables: license_label, extended_mapping, license_definitions
    """
    global eperson_id

    #import license_label and license_extended_mapping
    files = ['license_label.json', 'license_label_extended_mapping.json']
    end_points = ['licenses/import/labels', 'licenses/import/extendedMapping']
    for f, e in zip(files, end_points):
        json_a = read_json(f)
        if json_a:
            try:
                response = do_api_post(e, None, json_a)
                if response.status_code != 200:
                    log('Import ' + f + ': status code ' + str(response.status_code))
            except Exception as e:
                json_e = json.loads(e.args[0])
                log('Import ' + f + ': status code ' + str(json_e['status']))

    #import license_definition
    json_a = read_json('license_definition.json')
    if json_a:
        for data in json_a:
            data['eperson_id'] = eperson_id[data['eperson_id']]
        try:
            response = do_api_post( 'licenses/import/licenses', None, json_a)
            if response.status_code != 200:
                log('POST request ' + response.url + ' failed. Status code ' + str(response.status_code))
        except Exception as e:
            json_e = json.loads(e.args[0])
            log('POST request ' + json_e['path'] + ' failed. Status code ' + str(json_e['status']))

    print("License_label, Extended_mapping, License_definitions were successfully imported!")

def import_registrationdata():
    """
    Import data into database.
    Mapped tables: registrationdata
    """
    json_a = read_json('registrationdata.json')
    if json_a:
        for i in json_a:
            json_p = {'email' : i['email']}
            try:
                do_api_post('eperson/registrations', None, json_p)
            except Exception as e:
                json_e = json.loads(e.args[0])
                log('POST request' + json_e['path'] + ' for email: ' + i['email'] + ' failed. Status: ' +
                    str(json_e['status']))
    print("Registration data was successfully imported!")

def import_bitstreamformatregistry():
    global bitstreamformat_id
    #read all existing data from bitstreamformatregistry
    shortDesc2Id = dict()
    try:
        response = do_api_get_all('core/bitstreamformats')
        bitstreamformat = convert_response_to_json(response)['_embedded']['bitstreamformats']
        if bitstreamformat:
            for i in bitstreamformat:
                shortDesc2Id[i['shortDescription']] = i['id']

        json_a = read_json('bitstreamformatregistry.json')
        if json_a:
            for i in json_a:
                level = i['support_level']
                if level == 0:
                    level_str = "UNKNOWN"
                elif level == 1:
                    level_str = "KNOWN"
                elif level == 2:
                    level_str = "SUPPORTED"
                else:
                    log('Unsupported bitstream format registry id: ' + str(level))
                    continue

                json_p = {'mimetype': i['mimetype'], 'description': i['description'],
                          'shortDescription': i['short_description'], 'supportLevel': level_str,
                          'internal': i['internal']}
                try:
                    response = do_api_post('core/bitstreamformats', None, json_p)
                    bitstreamformat_id[i['bitstream_format_id']] = convert_response_to_json(response)['id']
                except:
                    if response.status_code == 200 or response.status_code == 201:
                        bitstreamformat_id[i['bitstream_format_id']] = shortDesc2Id[i['short_description']]
                        log('Bitstreamformatregistry with short description ' + i[
                            'short_description'] + ' already exists in database!')
                    else:
                        log('POST request ' + response.url + ' for id: ' + str(i['bitstream_format_id']) +
                            ' failed. Status: ' + str(response.status_code))
    except:
        log('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))

    print("Bitstream format registry was successfully imported!")

def import_epersongroup():
    """
    Import data into database.
    Mapped tables: epersongroup
    """
    global group_id
    json_a = read_json('epersongroup.json')
    # group Administrator and Anonymous already exist
    # we need to remember their id
    try:
        response = do_api_get_all('eperson/groups')
        existing_data = convert_response_to_json(response)['_embedded']['groups']
    except:
        log('GET request ' + response.url + ' failed.')

    if existing_data:
        for i in existing_data:
            if i['name'] == 'Anonymous':
                group_id[0] = [i['id']]
            elif i['name'] == 'Administrator':
                group_id[1] = [i['id']]
            else:
                log('Unrecognized eperson group ' + i['name'])

    if json_a:
        for i in json_a:
            id = i['eperson_group_id']
            # group Administrator and Anonymous already exist
            # group is created with dspace object too
            if id != 0 and id != 1 and id not in group_id:
                # get group metadata
                metadata_group = get_metadata_value(6, i['eperson_group_id'])
                name = metadata_group['dc.title'][0]['value']
                del metadata_group['dc.title']
                # the group_metadata contains the name of the group
                json_p = {'name': name, 'metadata': metadata_group}
                try:
                    response = do_api_post('eperson/groups', None, json_p)
                    group_id[i['eperson_group_id']] = [
                        convert_response_to_json(response)['id']]
                except:
                    log('POST request ' + response.url + ' for id: ' + str(i['eperson_group_id']) +
                        ' failed. Status: ' + str(response.status_code))

    print("Eperson group was successfully imported!")

def import_eperson():
    """
    Import data into database.
    Mapped tables: eperson, metadatavalue
    """
    global eperson_id
    #read user_registration
    json_a = read_json("user_registration.json")
    #create dict from user registrations
    user_reg = dict()
    if json_a:
        for i in json_a:
            user_reg[i['eperson_id']] = {'organization': i['organization'], 'confirmation': i['confirmation']}

    json_a = read_json('eperson.json')
    counter = 0
    if json_a:
        for i in json_a:
            if counter % 500 == 0:
                rest_proxy.reauthenticated()
            counter += 1
            metadata = get_metadata_value(7, i['eperson_id'])
            json_p = {'selfRegistered': i['self_registered'], 'requireCertificate' : i['require_certificate'],
                      'netid' : i['netid'], 'canLogIn' : i['can_log_in'], 'lastActive' : i['last_active'],
                      'email' : i['email'], 'password' : i['password']}
            if metadata:
                json_p['metadata'] = metadata
            if i['eperson_id'] in user_reg:
                param = user_reg[i['eperson_id']]
                param['userRegistration'] = True
            else:
                param = {'userRegistration': False}
            param['selfRegistered'] = i['self_registered']
            param['lastActive'] = i['last_active']
            try:
                response = do_api_post('clarin/import/eperson', param, json_p)
                eperson_id[i['eperson_id']] = convert_response_to_json(response)['id']
            except:
                log('POST request ' + response.url + ' for id: ' + str(i['eperson_id']) +
                    ' failed. Status: ' + str(response.status_code))

    print("Eperson was successfully imported!")

def import_group2group():
    """
    Import data into database.
    Mapped tables: group2group
    """
    global group_id
    json_a = read_json('group2group.json')
    if json_a:
        for i in json_a:
            try:
                do_api_post('clarin/eperson/groups/' + group_id[i['parent_id']][0] + '/subgroups', None,
                    const.API_URL + 'eperson/groups/' + group_id[i['child_id']][0])
            except Exception as e:
                json_e = json.loads(e.args[0])
                log('POST request ' + json_e['path'] + ' failed. Status: ' + str(json_e['status']))
    print("Group2group was successfully imported!")

def import_group2eperson():
    """
    Import data into database.
    Mapped tables: epersongroup2eperson
    """
    global group_id, eperson_id
    json_a = read_json('epersongroup2eperson.json')
    if json_a:
        for i in json_a:
            try:
                do_api_post('clarin/eperson/groups/' + group_id[i['eperson_group_id']][0] + '/epersons', None,
                    const.API_URL + 'eperson/groups/' + eperson_id[i['eperson_id']])
            except Exception as e:
                json_e = json.loads(e.args[0])
                log('POST request ' + json_e['path'] + ' failed. Status: ' + str(json_e['status']))

    print("Epersongroup2eperson was successfully imported!")

def import_metadataschemaregistry():
    """
    Import data into database.
    Mapped tables: metadataschemaregistry
    """
    global metadata_schema_id
    # get all existing data from database table
    try:
        response = do_api_get_all('core/metadataschemas')
        existing_data = convert_response_to_json(response)['_embedded']['metadataschemas']
    except:
        log('GET request ' + response.url + ' failed.')


    json_a = read_json('metadataschemaregistry.json')
    if  json_a:
        for i in json_a:
            json_p = {'namespace' : i['namespace'], 'prefix' : i['short_id']}
            #prefix has to be unique
            try:
                response = do_api_post('core/metadataschemas', None, json_p)
                metadata_schema_id[i['metadata_schema_id']] = convert_response_to_json(response)['id']
            except:
                found = False
                if existing_data:
                    for j in existing_data:
                        if j['prefix'] == i['short_id']:
                            metadata_schema_id[i['metadata_schema_id']] = j['id']
                            log('Metadataschemaregistry with prefix: ' + i['short_id']
                                + 'already exists in database!')
                            found = True
                            break
                if not found:
                    log('POST request ' + response.url + ' for id: ' + str(
                    i['metadata_schema_id']) + ' failed. Status: ' + str(response.status_code))
    print("MetadataSchemaRegistry was successfully imported!")

def import_metadatafieldregistry():
    """
    Import data into database.
    Mapped tables: metadatafieldregistry
    """
    global metadata_schema_id, metadata_field_id
    try:
        response = do_api_get_all('core/metadatafields')
        existing_data = convert_response_to_json(response)['_embedded']['metadatafields']
    except:
        log('GET request ' + response.url + ' failed. Status: ' + str(response.status_code))

    json_a = read_json('metadatafieldregistry.json')
    if json_a:
        for i in json_a:
            json_p = {'element' : i['element'], 'qualifier' : i['qualifier'], 'scopeNote' : i['scope_note']}
            param = {'schemaId': metadata_schema_id[i['metadata_schema_id']]}
            #element and qualifier have to be unique
            try:
                response = do_api_post('core/metadatafields', param, json_p)
                metadata_field_id[i['metadata_field_id']] = convert_response_to_json(response)['id']
            except:
                found = False
                if existing_data:
                    for j in existing_data:
                        if j['element'] == i['element'] and j['qualifier'] == i['qualifier']:
                            metadata_field_id[i['metadata_field_id']] = j['id']
                            log('Metadatafieldregistry with element: ' + i['element'] + ' already exists in database!')
                            found = True
                            break
                if not found:
                    log('POST request ' + response.url + ' for id: ' + str(
                        i['metadata_field_id']) + ' failed. Status: ' + str(response.status_code))
    print("MetadataFieldRegistry was successfully imported!")

def import_community():
    """
    Import data into database.
    Mapped tables: community, community2community, metadatavalue, handle
    """
    global group_id, metadatavalue, handle, community_id, community2logo
    json_comm = read_json('community.json')
    json_comm2comm = read_json('community2community.json')
    parent = dict()
    child = dict()
    if json_comm2comm:
        for i in json_comm2comm:
            parent_id = i['parent_comm_id']
            child_id = i['child_comm_id']
            if parent_id in parent.keys():
                parent[parent_id].append(child_id)
            else:
                parent[parent_id] = [child_id]
            if child_id in child.keys():
                child[child_id].append(parent_id)
            else:
                child[child_id] = parent_id

    if json_comm:
        counter = 0
        while json_comm:
            json_p = {}
            #process community only when:
            #comm is not parent and child
            #comm is parent and not child
            #parent comm exists
            #else process it later
            i = json_comm[counter]
            i_id = i['community_id']
            if (i_id not in parent.keys() and i_id not in child.keys()) or i_id not in child.keys() or child[i_id] in community_id.keys():
                #resource_type_id for community is 4
                if (4, i['community_id']) in handle:
                    handle_comm = handle[(4, i['community_id'])][0]
                    json_p = {'handle': handle_comm['handle']}
                metadatavalue_comm = get_metadata_value(4, i['community_id'])
                if metadatavalue_comm:
                    json_p = {'metadata': metadatavalue_comm}
                #create community
                parent_id = None
                if i_id in child:
                    parent_id = {'parent': community_id[child[i_id]]}
                try:
                    response = do_api_post('core/communities', parent_id, json_p)
                    resp_community_id = convert_response_to_json(response)['id']
                    community_id[i['community_id']] = resp_community_id
                except:
                    log('POST request ' + response.url + ' for id: ' + str(i_id) + ' failed. Status: ' +
                        str(response.status_code))

                #add to community2logo, if community has logo
                if i["logo_bitstream_id"] != None:
                    community2logo[i_id] = i["logo_bitstream_id"]

                # create admingroup
                if i['admin'] != None:
                    try:
                        response =  do_api_post('core/communities/' + resp_community_id + '/adminGroup', None, {})
                        group_id[i['admin']] = [convert_response_to_json(response)['id']]
                    except:
                        log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
                del json_comm[counter]
            else:
                counter += 1
            if counter == len(json_comm):
                counter = 0

    print("Community and Community2Community were successfully imported!")

def import_collection():
    """
    Import data into database.
    Mapped tables: collection, community2collection, metadatavalue, handle
    """
    global group_id, metadatavalue, handle, community_id, collection_id, collection2logo, eperson_id

    json_a = read_json('collection.json')
    comm_2_coll_json = read_json('community2collection.json')
    coll2comm = dict()
    if comm_2_coll_json:
        for i in comm_2_coll_json:
            coll2comm[i['collection_id']] = i['community_id']

    #because the role DEFAULT_READ is without old group id in collection
    coll2group = dict()
    metadata_json = read_json('metadatavalue.json')

    if metadata_json:
        for i in metadata_json:
            if i['resource_type_id'] == 6 and 'COLLECTION_' in i['text_value'] and '_DEFAULT_READ' in i['text_value']:
                text = i['text_value']
                positions = [ind for ind, ch in enumerate(text) if ch == '_']
                coll2group[int(text[positions[0] + 1: positions[1]])] = i['resource_id']
    if json_a:
        for i in json_a:
            json_p = {}
            metadata_col = get_metadata_value(3, i['collection_id'])
            if metadata_col:
                json_p['metadata'] = metadata_col
            if (3, i['collection_id']) in handle:
                handle_col = handle[(3, i['collection_id'])][0]
                json_p['handle'] = handle_col['handle']
            #TODO
            #missing submitter
            #, 'submitter': eperson_id[i['submitter']]
            params = {'parent': community_id[coll2comm[i['collection_id']]]}
            try:
                response = do_api_post('core/collections', params, json_p)
                coll_id = convert_response_to_json(response)['id']
                collection_id[i['collection_id']] = coll_id
            except:
                log('POST request ' + response.url + ' for id: ' + str(i['collection_id']) + 'failed. Status: ' +
                    str(response.status_code))

            #add to collection2logo, if collection has logo
            if i["logo_bitstream_id"] != None:
                community2logo[i['collection_id']] = i["logo_bitstream_id"]

            #greate group
            #template_item_id, workflow_step_1, workflow_step_3, admin are not implemented,
            # because they are null in all data
            if i['workflow_step_2']:
                try:
                    response = do_api_post('core/collections/' + coll_id + '/workflowGroups/editor', None, {})
                    group_id[i['workflow_step_2']] = [convert_response_to_json(response)['id']]
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
            if i['submitter']:
                try:
                    response = do_api_post('core/collections/' + coll_id + '/submittersGroup', None, {})
                    group_id[i['submitter']] = [convert_response_to_json(response)['id']]
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
            if i['collection_id'] in coll2group:
                try:
                    response = do_api_post('core/collections/' + coll_id + '/bitstreamReadGroup', None, {})
                    group_id[coll2group[i['collection_id']]] = [convert_response_to_json(response)['id']]
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
                try:
                    response = do_api_post('core/collections/' + coll_id + '/itemReadGroup', None, {})
                    group_id[coll2group[i['collection_id']]].append(convert_response_to_json(response)['id'])
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

    print("Collection and Community2collection were successfully imported!")

def import_item():
    """
    Import data into database.
    Mapped tables: item, collection2item,workspaceitem, cwf_workflowitem, metadata, handle
    """
    global workspaceitem_id
    rest_proxy.reauthenticated()
    #create dict from items by item id
    json_a = read_json("item.json")
    items = dict()
    if json_a:
        for i in json_a:
            items[i['item_id']] = i

    #create item and workspaceitem
    json_a = read_json("workspaceitem.json")
    if json_a:
        counter = 0
        for i in json_a:
            if counter % 500 == 0:
                rest_proxy.reauthenticated()
            counter += 1
            item = items[i['item_id']]
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'], i['multiple_files'],
                                 i['stage_reached'], i['page_reached'])
            del items[i['item_id']]

    print("Workspaceitem was successfully imported!")

    #create workflowitem
    #workflowitem is created from workspaceitem
    #-1, because the workflowitem doesn't contain this attribute
    json_a = read_json('workflowitem.json')
    counter = 0
    if json_a:
        for i in json_a:
            item = items[i['item_id']]
            if counter % 500 == 0:
                rest_proxy.reauthenticated()
            counter += 1
            import_workspaceitem(item, i['collection_id'], i['multiple_titles'], i['published_before'],
                                 i['multiple_files'], -1, -1)
            #create workflowitem from created workspaceitem
            params = {'id' : str(workspaceitem_id[i['item_id']])}
            try:
                response = do_api_post('clarin/import/workflowitem', params, None)
            except:
                log('POST request ' + response.url + ' for id: ' + str(i['item_id']) + ' failed. Status: '
                    + str(response.status_code))
            del items[i['item_id']]

    print("Cwf_workflowitem was successfully imported!")

    rest_proxy.reauthenticated()
    counter = 0
    #create other items
    for i in items.values():
        json_p = {'discoverable': i['discoverable'], 'inArchive': i['in_archive'],
                  'lastModified': i['last_modified'], 'withdrawn' : i['withdrawn']}
        metadata_item = get_metadata_value(2, i['item_id'])
        if metadata_item:
            json_p['metadata'] = metadata_item
        if i['item_id'] in handle:
            json_p['handle'] = handle[(2, i['item_id'])]
        params = {'owningCollection': collection_id[i['owning_collection']],
                  'epersonUUID': eperson_id[i['submitter_id']]}
        #we have to do reauthorization after some time
        if counter % 500 == 0:
            rest_proxy.reauthenticated()
        counter += 1
        try:
            response = do_api_post('clarin/import/item', params, json_p)
            response_json = convert_response_to_json(response)
            item_id[i['item_id']] = response_json['id']
        except:
            log('POST request ' + response.url + ' for id: ' + str(i['item_id'] + ' failed. Status: ' +
                                                                   str(response.status_code)))
    print("Item and Collection2item were successfully imported!")

def import_workspaceitem(item, owningCollectin, multipleTitles, publishedBefore, multipleFiles, stagereached,
                         pageReached):
    """
    Auxiliary method for import item.
    Import data into database.
    Mapped tables: workspaceitem, metadata, handle
    """
    global workspaceitem_id, item_id, collection_id, eperson_id

    json_p = {'discoverable': item['discoverable'], 'inArchive': item['in_archive'],
              'lastModified': item['last_modified'], 'withdrawn' : item['withdrawn']}
    metadata_item = get_metadata_value(2, item['item_id'])
    if metadata_item:
        json_p['metadata'] = metadata_item

    if item['item_id'] in handle:
        json_p['handle'] = handle[(2, item['item_id'])]
    #the params are workspaceitem attributes
    params = {'owningCollection': collection_id[owningCollectin],
              'multipleTitles': multipleTitles,
              'publishedBefore': publishedBefore,
              'multipleFiles': multipleFiles, 'stageReached': stagereached,
              'pageReached': pageReached,
              'epersonUUID': eperson_id[item['submitter_id']]}
    try:
        response = do_api_post('clarin/import/workspaceitem', params, json_p)
        id = convert_response_to_json(response)['id']
        workspaceitem_id[item['item_id']] = id
        try:
            response = rest_proxy.d.api_get(const.API_URL + 'clarin/import/' + str(id) + "/item", None, None)
            item_id[item['item_id']] = convert_response_to_json(response)['id']
        except:
            log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))
    except:
        log('POST request ' + response.url + ' for id: ' + str(item['item_id']) +
            ' failed. Status: ' + str(response.status_code))


def import_bundle():
    """
    Import data into database.
    Mapped tables: item2bundle, bundle
    """
    global item_id, bundle_id
    #load item2bundle into dict
    json_a = read_json("item2bundle.json")
    item2bundle = dict()
    if json_a:
        for i in json_a:
            if i['item_id'] in item2bundle:
                item2bundle[i['item_id']].append(i['bundle_id'])
            else:
                item2bundle[i['item_id']] = [i['bundle_id']]

    #load bundles and map bundles to their primary bitstream ids
    json_a = read_json("bundle.json")
    if json_a:
        for i in json_a:
            if i['primary_bitstream_id']:
                primaryBitstream[i['primary_bitstream_id']] = i['bundle_id']

    #import bundle without primary bitstream id
    counter = 0
    if item2bundle:
        for item in item2bundle.items():
            for bundle in item[1]:
                if counter % 500 == 0:
                    rest_proxy.reauthenticated()
                counter += 1
                json_p = dict()
                metadata_bundle = get_metadata_value(1, bundle)
                if metadata_bundle:
                    json_p['metadata'] = metadata_bundle
                    json_p['name'] = metadata_bundle['dc.title'][0]['value']

                try:
                    response = do_api_post('core/items/' + str(item_id[item[0]]) + "/bundles", None, json_p)
                    bundle_id[bundle] = convert_response_to_json(response)['uuid']
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

    print("Bundle and Item2Bundle were successfully imported!")

def import_bitstream():
    """
    Import data into database.
    Mapped tables: bitstream, bundle2bitstream, metadata, most_recent_checksum and checksum_result
    """
    global bitstreamformat_id, primaryBitstream, bitstream2bundle
    #load bundle2bitstream
    json_a = read_json("bundle2bitstream.json")
    if json_a:
        for i in json_a:
            bitstream2bundle[i['bitstream_id']] = i['bundle_id']

    #load and import bitstreams
    json_a = read_json("bitstream.json")
    if json_a:
        for i in json_a:
            json_p = dict()
            metadata_bitstream = get_metadata_value(0, i['bitstream_id'])
            if metadata_bitstream:
                json_p['metadata'] = metadata_bitstream
            json_p['sizeBytes'] = i['size_bytes']
            json_p['checkSum'] = {'checkSumAlgorithm': i['checksum_algorithm'], 'value': i['checksum']}
            params = {'internal_id': i['internal_id'],
                      'storeNumber': i['store_number'],
                      'bitstreamFormat': bitstreamformat_id[i['bitstream_format_id']],
                      'deleted': i['deleted'],
                      'sequenceId': i['sequence_id'],
                      'bundle_id': None,
                      'primaryBundle_id': None}

            #if bitstream has bundle, set bundle_id from None to id
            if i['bitstream_id'] in bitstream2bundle:
                params['bundle_id'] = bundle_id[bitstream2bundle[i['bitstream_id']]]

            #if bitstream is primary bitstream of some bundle, set primaryBundle_id from None to id
            if i['bitstream_id'] in primaryBitstream:
                params['primaryBundle_id'] = bundle_id[primaryBitstream[i['bitstream_id']]]
            try:
                response = do_api_post('clarin/import/core/bitstream', params, json_p)
                bitstream_id[i['bitstream_id']] = convert_response_to_json(response)['id']
            except:
                log('POST request ' + response.url + ' for id: ' + str(i['bitstream_id']) + ' failed. Status: ' +
                    str(response.status_code))

    #add logos (bitstreams) to collections and communities
    add_logo_to_community()
    add_logo_to_collection()

    #do bitstream checksum
    #fill the tables: most_recent_checksum and checksum_result based on imported bitstreams
    try:
        do_api_post('clarin/import/core/bitstream/checksum', None, None)
    except Exception as e:
        json_e = json.loads(e.args[0])
        log('POST request ' + json_e['path'] + ' failed. Status: ' + str(json_e['status']))
    print("Bitstream, bundle2bitstream, most_recent_checksum and checksum_result were successfully imported!")

def add_logo_to_community():
    """
    Add bitstream to community as community logo.
    Logo has to exist in database.
    """
    global community2logo, bitstream_id, community_id
    if community2logo:
        for key, value in community2logo.items():
            if key in community_id and value in bitstream_id:
                params = {'community_id': community_id[key], 'bitstream_id': bitstream_id[value]}
                try:
                    response = do_api_post("clarin/import/logo/community", params, None)
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

def add_logo_to_collection():
    """
    Add bitstream to collection as collection logo.
    Logo has to exist in database.
    """
    global collection2logo, bitstream_id, collection_id
    if collection2logo:
        for key, value in collection2logo.items():
            if key in collection_id and value in bitstream_id:
                params = {'collection_id': collection_id[key], 'bitstream_id': bitstream_id[value]}
                try:
                    response = do_api_post("clarin/import/logo/collection", params, None)
                except:
                    log('POST request ' + response.url + ' failed. Status: ' + str(response.status_code))

def import_handle_with_url():
    """
    Import handles into database with url.
    Other handles are imported by dspace objects.
    Mapped table: handles
    """
    global handle
    #if handle is empty, read handle file as json
    if not handle:
        read_handle()
    #handle with defined url has key (None, None)
    if (None, None) in handle:
        handles_url = handle[(None, None)]
        for i in handles_url:
            json_p = {'handle': i['handle'], 'url': i['url']}
            try:
                response = do_api_post('core/handles', None, json_p)
            except:
                log('POST response ' + response.url + ' failed. Status: ' + str(response.status_code))

    print("Handles with url were successfully imported!")

def import_epersons_and_groups():
    """
    Import part of dspace: epersons and groups.
    """
    import_registrationdata()
    import_epersongroup()
    import_group2group()
    import_eperson()
    import_group2eperson()

def import_metadata():
    """
    Import part of dspace: metadata
    """
    import_metadataschemaregistry()
    import_metadatafieldregistry()

def import_hierarchy():
    """
    Import part of dspace: hierarchy
    """
    import_community()
    import_collection()

def import_bundles_and_bitstreams():
    """
    Import part of dspace: bundles and bitstreams
    """
    import_item()
    import_bitstreamformatregistry()
    import_bundle()
    #import_bitstream()

#call
print("Data migraton started!")
#at the beginning
read_metadata()
read_handle()
#not depends on the ather tables
import_handle_with_url()

#you have to call together
import_metadata()
#import hierarchy has to call before import group
import_hierarchy()
import_epersons_and_groups()
import_licenses()
import_bundles_and_bitstreams()
print("Data migration is completed!")