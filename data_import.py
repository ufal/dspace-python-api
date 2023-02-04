import json
import const
import psycopg2

from support.dspace_proxy import rest_proxy


def import_license(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    rest_proxy.d.api_post(url, None, json_array)

def import_licnses():
    test_data()
    import_license('licenses/import/labels', "license_label.json")
    import_license('licenses/import/extendedMapping', "license_label_extended_mapping.json")
    import_license('licenses/import/licenses', "license_definition.json")
    test_data()

def import_communities(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        response = rest_proxy.d.api_post(url, None, i)
        print(response)

def test_collection(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        response = rest_proxy.d.api_post(url, None, i)
        print(response)

def test_schema_version(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        response = rest_proxy.d.api_post(url, None, i)
        print(response)

def test_eperson(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        json_data = {'selfRegistered': i['self_registered'], 'requireCertificate' : i['require_certificate'], 'netid' : i['netid'], 'canLogIn' : i['can_log_in'], 'lastActive' : i['last_active'], 'email' : i['email'], 'password' : i['password']}
        response = rest_proxy.d.api_post(url, None, json_data)
        json.loads(response.content.decode('utf-8'))['id']

def test_group(request_mapping, file_name):
    url = const.API_URL + request_mapping
    x = open("C:/dspace-blackbox-testing/data/" + file_name)
    json_array = json.load(x)
    x.close()
    if json_array is None:
        return
    for i in json_array:
        response = rest_proxy.d.api_post(url, None, i)
        print(response)
def import_schema_version():
    test_data()
    test_schema_version('versioning/versions', "schema_version.json")
    test_data()

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

def import_group():
    test_group('eperson/groups', 'epersongroup.json')
def import_collection():
    test_data()
    test_collection('core/collections', "collection.json")
    test_data()

#import_community()
#import_collection()
#import_licnses()
#import_schema_version()
#import_eperson()
import_group()
