import json

import const
from support.logs import log
from support.dspace_proxy import rest_proxy

#global param
eperson_id = dict()
group_id = dict()
metadata_schema_id = dict()
metadata_field_id = dict()
community_id = dict()
metadatavalue = dict()
handle = dict()

#using functions
def read_json(file_name):
    """
    Read data from file as json.
    @param file_name: file name
    @return: data as json
    """
    x = open(const.FILE_PATH + file_name)
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
    #control of responce status is missing
    log('Api post by ' + url + ' was successfully done!')
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
    #control of response status is missing
    log('Api get by ' + url + ' was successfully done!')
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
    #control of response satus is missing
    log('Api get by url ' + url + ' was successfully done!')
    return response

def convert_response_to_json(response):
    """
    Convert response to json.
    @param response: response from api call
    @return: json created from response
    """
    return json.loads(response.content.decode('utf-8'))

def get_metadata_by_type(resource_type_id):
    """
    Return metadata with resource_type_id.
    @param resource_type_id: id of resource type
    @return: dictionary resource_id : text_value
    """
    json_a = read_json('metadatavalue.json')
    metadata = dict()
    for i in json_a:
        if i['resource_type_id'] == resource_type_id:
            metadata[i['resource_id']] = i['text_value']
    return metadata


def read_metadata():
    """
    Read metadatavalue as json and
    convert it to dictionary with tuple key: resource_type_id and resource_id.
    """
    global metadatavalue
    metadatavalue_json = read_json('metadatavalue.json')
    for i in metadatavalue_json:
        key = (i['resource_type_id'], i['resource_id'])
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
    metadatavalue_obj = metadatavalue[(old_resource_type_id, old_resource_id)]
    if metadatavalue_obj:
        #create list of object metadata
        for i in metadatavalue_obj:
            # get metadatafield
            metadatafield_json = convert_response_to_json(
                do_api_get_one('core/metadatafields', metadata_field_id[i['metadata_field_id']]))
            # get metadataschema
            metadataschema_json = convert_response_to_json(do_api_get_one('core/metadataschemas',
                                                           metadatafield_json['_embedded']['schema']['id']))
            #define and insert key and value of dict
            key = metadataschema_json['prefix'] + '.' + metadatafield_json['element']
            value = {'value' : i['text_value'], 'language' : i['text_lang'], 'authority' : i['authority'],
                           'confidence' : i['confidence'], 'place' : i['place']}
            if metadatafield_json['qualifier'] != None:
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
    files = ['license_label.json', 'license_label_extended_mapping.json', 'license_definition.json']
    end_points = ['licenses/import/labels', 'licenses/import/extendedMapping', 'licenses/import/licenses']
    for f,e in zip(files, end_points):
        do_api_post(e, None, read_json(f))

def import_registrationdata():
    """
    Import data into database.
    Mapped tables: registrationdata
    """
    json_a = read_json('registrationdata.json')
    for i in json_a:
        json_p = {'email' : i['email']}
        do_api_post('eperson/registrations', None, json_p)

def import_bitstreamformatregistry():
    json_a = read_json('bitstreamformatregistry.json')
    for i in json_a:
        level = i['support_level']
        if level == 0:
            level_str = "UNKNOWN"
        elif level == 1:
            level_str = "KNOWN"
        elif level == 2:
            level_str = "SUPPORTED"
        else:
            raise Exception("error")

        json_p = {'mimetype' : i['mimetype'], 'description' : i['description'],
                     'shortDescription' : i['short_description'], 'supportLevel' : level_str,
                     'internal' : i['internal']}
        try:
            do_api_post('core/bitstreamformats', None, json_p)
        except:
            log('Bitstreamformatregistry with short description ' + i['short_description'] + ' already exists in database!')


def import_epersongroup():
    """
    Import data into database.
    Mapped tables: epersongroup
    """
    global group_id
    metadata = get_metadata_by_type(6)
    json_a = read_json('epersongroup.json')
    # group Administrator and Anonymous already exist
    # we need to remember their id
    existing_data = convert_response_to_json(do_api_get_all('eperson/groups'))['_embedded']['groups']
    for i in existing_data:
        if i['name'] == 'Anonymous':
            group_id[0] = i['id']
        if i['name'] == 'Administrator':
            group_id[1] = i['id']
    for i in json_a:
        # group Administrator and Anonymous already exist
        if i['eperson_group_id'] != 0 and i['eperson_group_id'] != 1:
            #get group metadata
            metadata_group = get_metadata_value(6, i['eperson_group_id'])
            json_p = {'name': metadata[i['eperson_group_id']], 'metadata' : metadata_group}
            group_id[i['eperson_group_id']] = convert_response_to_json(do_api_post('eperson/groups', None, json_p))['id']


def import_eperson():
    """
    Import data into database.
    Mapped tables: eperson
    """
    global eperson_id
    json_a = read_json('eperson.json')
    for i in json_a:
        metadata = get_metadata_value(7, i['eperson_id'])
        json_p = {'selfRegistered': i['self_registered'], 'requireCertificate' : i['require_certificate'],
                  'netid' : i['netid'], 'canLogIn' : i['can_log_in'], 'lastActive' : i['last_active'],
                  'email' : i['email'], 'password' : i['password'], 'metadata' : metadata}
        eperson_id[i['eperson_id']] = convert_response_to_json(do_api_post('eperson/epersons', None, json_p))['id']

def import_group2group():
    """
    Import data into database.
    Mapped tables: group2group
    """
    global group_id
    json_a = read_json('group2group.json')
    for i in json_a:
        do_api_post('clarin/eperson/groups/' + group_id[i['parent_id']] + '/subgroups', None,
                    const.API_URL + 'eperson/groups/' + group_id[i['child_id']])

def import_group2eperson():
    """
    Import data into database.
    Mapped tables: epersongroup2eperson
    """
    global group_id, eperson_id
    json_a = read_json('epersongroup2eperson.json')
    for i in json_a:
        do_api_post('clarin/eperson/groups/' + group_id[i['eperson_group_id']] + '/epersons', None,
                    const.API_URL + 'eperson/groups/' + eperson_id[i['eperson_id']])

def import_metadataschemaregistry():
    """
    Import data into database.
    Mapped tables: metadataschemaregistry
    """
    global metadata_schema_id
    # get all existing data from database table
    existing_data = convert_response_to_json(do_api_get_all('core/metadataschemas'))['_embedded']['metadataschemas']
    json_a = read_json('metadataschemaregistry.json')
    for i in json_a:
        json_p = {'namespace' : i['namespace'], 'prefix' : i['short_id']}
        #prefix has to be unique
        try:
            metadata_schema_id[i['metadata_schema_id']] = convert_response_to_json(do_api_post('core/metadataschemas', None, json_p))['id']
        except:
            for j in existing_data:
                if j['prefix'] == i['short_id']:
                    metadata_schema_id[i['metadata_schema_id']] = j['id']
                    break


def import_metadatafieldregistry():
    """
    Import data into database.
    Mapped tables: metadatafieldregistry
    """
    global metadata_schema_id, metadata_field_id
    existing_data = convert_response_to_json(do_api_get_all('core/metadatafields'))['_embedded']['metadatafields']
    json_a = read_json('metadatafieldregistry.json')
    for i in json_a:
        json_p = {'element' : i['element'], 'qualifier' : i['qualifier'], 'scopeNote' : i['scope_note']}
        param = {'schemaId': metadata_schema_id[i['metadata_schema_id']]}
        #element and qualifier have to be unique
        try:
            metadata_field_id[i['metadata_field_id']] = convert_response_to_json(do_api_post('core/metadatafields', param, json_p))['id']
        except:
            for j in existing_data:
                if j['element'] == i['element'] and j['qualifier'] == i['qualifier']:
                    metadata_field_id[i['metadata_field_id']] = j['id']
                    break



def import_community():
    """
    Import data into database.
    Mapped tables: community, community2community, metadatavalue
    """
    global group_id, metadatavalue, handle
    if not handle:
        read_handle()
    if not metadatavalue:
        read_metadata()

    #json_comm2comm = read_json('community2community.json')
    json_comm = read_json('community.json')

    for i in json_comm:
        #resource_type_id for community is 4
        handle_comm = handle[(4,i['community_id'])]
        metadatavalue_comm = metadatavalue[(4, i['community_id'])]
        admin_comm = group_id[i['admin']]
        json_p = {'handle' : handle_comm, 'admin' : admin_comm, 'metadata' : metadatavalue_comm}
        community_id[i['community_id']] = convert_response_to_json(do_api_post('core/communities', None, json_p))['id']

def import_collection():
    json_a = read_json('collection.json')
    for i in json_a:
        metadata_col = get_metadata_value(3, i['collection_id'])
        handle_col = handle[(3, i['collection_id'])]
        #missing submitter and logo
        json_p = {'handle' : handle_col , 'metadata' : metadata_col}
        response = rest_proxy.d.api_post('core/collections', None,json_p)
        print(response)

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
    handles_url = handle[(None, None)]
    for i in handles_url:
        json_p = {'handle': i['handle'], 'url': i['url']}
        do_api_post('core/handles', None, json_p)

def import_epersons_and_groups():
    """
    Import part of dspace: epersons and groups.
    """
    import_registrationdata()
    import_epersongroup()
    import_group2group()
    import_eperson()
    import_group2eperson()
    #missing epersongroup2workspace

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


#call
#import_licenses()
#import_registrationdata()
#import_handle_with_url()
#import_bitstreamformatregistry()

#you have to call together
import_metadata()
import_epersons_and_groups()
import_hierarchy()