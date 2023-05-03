import json
import const
import psycopg2

from support.dspace_proxy import rest_proxy

eperson_id = dict()
group_id = dict()
metadata_field_id = dict()

# def import_license(request_mapping, file_name):
#     url = const.API_URL + request_mapping
#     x = open("C:/dspace-blackbox-testing/data/" + file_name)
#     json_array = json.load(x)
#     x.close()
#     rest_proxy.d.api_post(url, None, json_array)

# def import_licnses():
#     test_data()
#     import_license('licenses/import/labels', "license_label.json")
#     import_license('licenses/import/extendedMapping', "license_label_extended_mapping.json")
#     import_license('licenses/import/licenses', "license_definition.json")
#     test_data()

def import_communities(request_mapping, file_name, handle_file_name, metadata_file_name):
    global metadata_field_id, group_id
    #read handle
    x = open("C:/dspace-blackbox-testing/data/" + handle_file_name)
    handle_json = json.load(x)
    x.close()
    #read metadata
    x = open("C:/dspace-blackbox-testing/data/" + metadata_file_name)
    metadata_json = json.load(x)
    x.close()

    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        #find handle
        for j in handle_json:
            if j['resource_type_id'] == 4 and j['resource_id'] == i['community_id']:
                handle = j['handle']
                break

        #metadatafield mapping is not done
        #find metadata
        metadata = dict()
        for j in metadata_json:
            if j['resource_id'] == i['community_id']:
                #get metadatafield
                response1 = rest_proxy.d.api_get(const.API_URL + 'core/metadatafields/' + str(metadata_field_id[j['metadata_field_id']]))
                #get metadatavalue
                response2 = rest_proxy.d.api_get(
                    const.API_URL + 'core/metadataschemas/' + str(json.loads(response2.content.decode('utf-8'))['schema']['id']))
                metadata[json.loads(response2.content.decode('utf-8'))['short_id'] + '.' +
                json.loads(response1.content.decode('utf-8'))['element'] + '.' + json.loads(response1.content.decode('utf-8'))['qualifier']] = {'value' : j['text_value'], 'language' : j['text_lang'], 'authority' : j['authority'], 'confidence' : j['confidence'], 'place' : j['place']}
        json_data = {'handle' : handle, 'admin' : group_id[i['eperson_group_id']], 'metadata' : metadata}
        response = rest_proxy.d.api_post(url, None, json_data)
        print(response)

def import_bitstreamformat(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        level = i['support_level']
        if level == 0:
            level_str = "UNKNOWN"
        elif level == 1:
            level_str = "KNOWN"
        elif level == 2:
            level_str = "SUPPORTED"
        else:
            raise Exception("error")

        json_data = {'mimetype' : i['mimetype'], 'description' : i['description'],
                     'shortDescription' : i['short_description'], 'supportLevel' : level_str,
                     'internal' : i['internal']}
        response = rest_proxy.d.api_post(url, None, json_data)
        print(response)

def import_metadataschemaRegistry(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    schema = dict()
    if json_array is None:
        return
    for i in json_array:
        json_data = {'namespace' : i['namespace'], 'prefix' : i['short_id']}
        response = rest_proxy.d.api_post(url, None, json_data)
        schema[i['metadata_schema_id']] = json.loads(response.content.decode('utf-8'))['id']
    import_metadataFieldRegistry('core/metadatafields', 'metadatafieldregistry.json', schema)
def import_metadataFieldRegistry(request_mapping, file_name, schema):
    global metadata_field_id
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        json_data = {'element' : i['element'], 'qualifier' : i['qualifier'], 'scopeNote' : i['scope_note']}
        param = {'schemaId': schema[i['metadata_schema_id']]}
        response = rest_proxy.d.api_post(url, param, json_data)
        metadata_field_id[i['metadata_field_id']] = json.loads(response.content.decode('utf-8'))['id']

def import_registrationdata(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        json_data = {'email' : i['email']}
        response = rest_proxy.d.api_post(url, None, json_data)


def import_items(request_mapping, file_name):
    #handle json, metadata
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        json_data = {'discoverable' : i['discoverable'], 'inArchive' : i['in_archive'], 'lastModified' : i['last_modified']}
        response = rest_proxy.d.api_post(url, None, json_data)
        print(response)



def test_collection(request_mapping, file_name):
    #handle, metadata, logo bitstream
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        response = rest_proxy.d.api_post(url, None, i)
        print(response)

def test_add_subgroup(request_mapping, file_name, mapping):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        #get child group
        childGroup = rest_proxy.d.api_get('')
        response = rest_proxy.d.api_post(url + mapping[i['parent_id']] + "/subgroups", None, i)
        print(response)

# def test_schema_version(request_mapping, file_name):
#     url = const.API_URL + request_mapping
#     x = open("C:/dspace-blackbox-testing/data/" + file_name)
#     json_array = json.load(x)
#     x.close()
#     if json_array is None:
#         return
#     for i in json_array:
#         response = rest_proxy.d.api_post(url, None, i)
#         print(response)

def test_eperson(request_mapping, file_name):
    global eperson_id
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    count = 0
    for i in json_array:
        count = count + 1
        json_data = {'selfRegistered': i['self_registered'], 'requireCertificate' : i['require_certificate'], 'netid' : i['netid'], 'canLogIn' : i['can_log_in'], 'lastActive' : i['last_active'], 'email' : i['email'], 'password' : i['password']}
        response = rest_proxy.d.api_post(url, None, json_data)
        eperson_id[i['eperson_id']] = json.loads(response.content.decode('utf-8'))['id']

#def test_group(request_mapping, file_name):
#    url = const.API_URL + request_mapping
#    x = open("C:/dspace-blackbox-testing/data/" + file_name)
#   json_array = json.load(x)
 #   x.close()
#    mapping = dict()
#   if json_array is None:
#        return
#    for i in json_array:
#        response = rest_proxy.d.api_post(url, None, i)
#        mapping[i['eperson_group_id']] = json.loads(response.content.decode('utf-8'))['id']
#        print(response)

    #registratondata("eperson/registrations", "registrationdata.json")

# def registratondata(request_mapping, file_name):
#     global eperson_id
#     url = const.API_URL + request_mapping
#     x = open("C:/dspace-blackbox-testing/data/" + file_name)
#     json_array = json.load(x)
#     x.close()
#     if json_array is None:
#         return
#     for i in json_array:
#         json_data = {'user': i['user'], 'email' : i['email']}
#         response = rest_proxy.d.api_post(url, None, i)

def test_group(request_mapping, file_name1, file_name2):
    global group_id
    url = const.API_URL + request_mapping

    # get all existing groups from database
    #response = rest_proxy.d.api_get(url)
    #groups = json.loads(response.content.decode('utf-8'))['_embedded']['groups']
    #group_dict = dict()
    #for i in groups:
    #    group_dict[i['name']] = i['id']
    x = open("C:/dspace-blackbox-testing/data/" + file_name1)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    metadata = dict()
    for i in json_array:
        if i['resource_type_id'] == 6:
            metadata[i['resource_id']] = i['text_value']

    x = open("C:/dspace-blackbox-testing/data/" + file_name2)
    json_array = json.load(x)
    x.close()
    #response = rest_proxy.d.api_post(url, None, i)
    for i in json_array:
        if i['eperson_group_id'] != 0 and i['eperson_group_id'] != 1:
            json_data = {'name': metadata[i['eperson_group_id']]}
            response = rest_proxy.d.api_post(url, None, json_data)
            group_id[i['eperson_group_id']] = json.loads(response.content.decode('utf-8'))['id']
    print()

    #subgrou
    #test_add_subgroup('eperson/groups', 'group2group.json', group_id)


#import_eperson()

# def test_handle(request_mapping, file_name1):
#     url = const.API_URL + request_mapping
#     x = open("C:/dspace-blackbox-testing/data/" + file_name1)
#     json_array = json.load(x)
#     x.close()
#     if json_array is None:
#         return
#     for i in json_array:
#         response = rest_proxy.d.api_post(url, None, i)
#     print()
#version history and versionhistory_item are null
# def test_versionhistory(request_mapping1, request_mapping2, file_name1, file_name2):
#     #versionhistory
#     url = const.API_URL + request_mapping1
#     x = open("C:/dspace-blackbox-testing/data/" + file_name1)
#     json_array = json.load(x)
#     x.close()
#     mapping = dict()
#     if json_array is None:
#         return
#     for i in json_array:
#         response = rest_proxy.d.api_post(url, None, i)
#         mapping[i['versionhistory_id']] = json.loads(response.content.decode('utf-8'))['id']
#
#    #versionhistory_item
#     url = const.API_URL + request_mapping2
#     x = open("C:/dspace-blackbox-testing/data/" + file_name2)
#     json_array = json.load(x)
#     x.close()
#     if json_array is None:
#         return
#     for i in json_array:
#
#         response = rest_proxy.d.api_post(url, None, i)
#         mapping[i['versionhistory_id']] = json.loads(response.content.decode('utf-8'))['id']

# def import_schema_version():
#     test_data()
#     test_schema_version('versioning/versions', "schema_version.json")
#     test_data()

def test_data():
    conn = psycopg2.connect(database="dspace",
                            host="localhost",
                            user="postgres",
                            password="dspace")
    print("Connection was successful!")

    cursor = conn.cursor()
    cursor.execute("SELECT * from eperson")
    data_ = cursor.fetchall()
    for i in data_:
        print(i)
    conn.close()




def import_community():
    test_data()
    import_communities('core/communities', "community.json")
    test_data()

def import_eperson():
    test_data()
    test_eperson('eperson/epersons', "eperson.json")
    test_data()

def import_collection():
    test_data()
    test_collection('core/collections', "collection.json")
    test_data()

def import_group():
    test_group('eperson/groups', "metadatavalue.json", "epersongroup.json")

#import_collection()
#import_licnses()
#import_schema_version()
#import_eperson()
import_group()
#import_metadata()

#registratondata("eperson/registrations", "registrationdata.json")
#import_items('core/items', 'item.json')
#test_handle('core/handles', 'handle.json')
#import_bitstreamformat('core/bitstreamformats', 'bitstreamformatregistry.json')
import_metadataschemaRegistry('core/metadataschemas', 'metadataschemaregistry.json')
#import_registrationdata('eperson/registrations','registrationdata.json')

import_communities('core/communities', 'community.json', 'handle.json', 'metadatavalue.json')