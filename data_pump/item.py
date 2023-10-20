import datetime
import logging

import const
from data_pump.sequences import connect_to_db
from data_pump.utils import read_json, convert_response_to_json, do_api_post, \
    save_dict_as_json
from data_pump.var_declarations import DC_RELATION_REPLACES_ID, DC_RELATION_ISREPLACEDBY_ID, DC_IDENTIFIER_URI_ID
from support.dspace_proxy import rest_proxy
from const import API_URL
from migration_const import WORKFLOWITEM_DICT, WORKSPACEITEM_DICT, ITEM_DICT

def import_item(metadata_class,
                handle_class,
                workflowitem_id_dict,
                workspaceitem_id_dict,
                item_id_dict,
                collection_id_dict,
                eperson_id_dict,
                statistics_dict,
                item_handle_item_metadata_dict,
                save_dict):
    """
    Import data into database.
    Mapped tables: item, collection2item, workspaceitem, cwf_workflowitem,
    metadata, handle
    """
    item_json_name = "item.json"
    workspaceitem_json_name = "workspaceitem.json"
    workflowitem_json_name = 'workflowitem.json'
    collection2table_name = "collection2item.json"
    item_url = 'clarin/import/item'
    workflowitem_url = 'clarin/import/workflowitem'
    item2collection_url = 'clarin/import/item/{item_uuid}/mappedCollections'
    imported_workspaceitem = 0
    imported_workflowitem = 0
    imported_item = 0
    # create dict from items by item id
    item_json_list = read_json(item_json_name)
    items_dict = {}
    if not item_json_list:
        logging.info("Item JSON is empty.")
        return
    for item in item_json_list:
        items_dict[item['item_id']] = item
    # statistics_dict['item'] = (len(item_json_list), 0)
    #
    # # create item and workspaceitem
    # workspaceitem_json_list = read_json(workspaceitem_json_name)
    # if workspaceitem_json_list is not None:
    #     for workspaceitem in workspaceitem_json_list:
    #         item = items_dict[workspaceitem['item_id']]
    #         import_workspaceitem(item, workspaceitem['collection_id'],
    #                              workspaceitem['multiple_titles'],
    #                              workspaceitem['published_before'],
    #                              workspaceitem['multiple_files'],
    #                              workspaceitem['stage_reached'],
    #                              workspaceitem['page_reached'],
    #                              metadata_class,
    #                              handle_class,
    #                              workspaceitem_id_dict,
    #                              item_id_dict,
    #                              collection_id_dict,
    #                              eperson_id_dict)
    #         imported_workspaceitem += 1
    #         del items_dict[workspaceitem['item_id']]
    #
    #     statistics_dict['workspaceitem'] = (len(workspaceitem_json_list),
    #                                         imported_workspaceitem)
    #     imported_item += imported_workspaceitem
    #     # save workspaceitem dict as json
    #     if save_dict:
    #         save_dict_as_json(WORKSPACEITEM_DICT, workspaceitem_id_dict)
    #     logging.info("Workspaceitem was successfully imported!")
    # else:
    #     logging.info("Workspaceitem JSON is empty.")
    # # create workflowitem
    # # workflowitem is created from workspaceitem
    # # -1, because the workflowitem doesn't contain this attribute
    # workflowitem_json_list = read_json(workflowitem_json_name)
    # if workflowitem_json_list is not None:
    #     for workflowitem in workflowitem_json_list:
    #         item = items_dict[workflowitem['item_id']]
    #         import_workspaceitem(item, workflowitem['collection_id'],
    #                              workflowitem['multiple_titles'],
    #                              workflowitem['published_before'],
    #                              workflowitem['multiple_files'],
    #                              -1,
    #                              -1,
    #                              metadata_class,
    #                              handle_class,
    #                              workspaceitem_id_dict,
    #                              item_id_dict,
    #                              collection_id_dict,
    #                              eperson_id_dict)
    #         # create workflowitem from created workspaceitem
    #         params = {'id': str(workspaceitem_id_dict[workflowitem['item_id']])}
    #         try:
    #             response = do_api_post(workflowitem_url, params, None)
    #             workflowitem_id_dict[workflowitem['workflow_id']] = \
    #                 response.headers['workflowitem_id']
    #             imported_workflowitem += 1
    #         except Exception as e:
    #             logging.error('POST request ' + workflowitem_url + ' for id: ' +
    #                           str(workflowitem['item_id']) + ' failed. Exception: ' +
    #                           str(e))
    #         del items_dict[workflowitem['item_id']]
    #
    #     # save workflow dict as json
    #     if save_dict:
    #         save_dict_as_json(WORKFLOWITEM_DICT, workflowitem_id_dict)
    #     statistics_val = (len(workflowitem_json_list), imported_workflowitem)
    #     statistics_dict['workflowitem'] = statistics_val
    #     imported_item += imported_workflowitem
    #     logging.info("Cwf_workflowitem was successfully imported!")
    # else:
    #     logging.info("Workflowitem JSON is empty.")
    #
    # # create other items
    # for item in items_dict.values():
    #     item_json_p = {
    #         'discoverable': item['discoverable'],
    #         'inArchive': item['in_archive'],
    #         'lastModified': item['last_modified'],
    #         'withdrawn': item['withdrawn']
    #     }
    #     metadatvalue_item_dict = metadata_class.get_metadata_value(2, item['item_id'])
    #     if metadatvalue_item_dict:
    #         item_json_p['metadata'] = metadatvalue_item_dict
    #     handle_item = handle_class.get_handle(2, item['item_id'])
    #     if handle_item is not None:
    #         item_json_p['handle'] = handle_item
    #     params = {
    #         'owningCollection': collection_id_dict[item['owning_collection']],
    #         'epersonUUID': eperson_id_dict[item['submitter_id']]
    #     }
    #     try:
    #         response = do_api_post(item_url, params, item_json_p)
    #         response_json = convert_response_to_json(response)
    #         item_id_dict[item['item_id']] = response_json['id']
    #         imported_item += 1
    #     except Exception as e:
    #         logging.error('POST request ' + item_url + ' for id: ' +
    #                       str(item['item_id']) + ' failed. Exception: ' + str(e))
    #
    # # Import collection2item table - only items which are mapped in more collections
    # # Add another collection into Item only if another collection is not owning_collection
    # collection2table_json_list = read_json(collection2table_name)
    # coll_2_item_dict = {}
    # items_with_more_colls = {}
    # # Find items which are mapped in more collections and store them into dictionary in this way
    # # {'item_uuid': [collection_uuid_1, collection_uuid_2]}
    # for collection2table in collection2table_json_list:
    #     # Every item should have mapped only one collection - the owning collection except the items which
    #     # are mapped into more collections
    #     item_uuid = item_id_dict[collection2table['item_id']]
    #     collection_uuid = collection_id_dict[collection2table['collection_id']]
    #     if item_uuid in coll_2_item_dict:
    #         # Add another collection into dict to get all collections for current Item
    #         coll_2_item_dict[item_uuid].append(collection_id_dict[collection2table['collection_id']])
    #         # Add item UUID and collection UUID into list in this way {`item_uuid`: `collection_uuid`}
    #         items_with_more_colls[item_uuid] = collection_uuid
    #         continue
    #     coll_2_item_dict[item_uuid] = [collection_uuid]
    #
    # # Call Vanilla REST endpoint which add relation between Item and Collection into the collection2item table
    # for item_with_more_coll_uuid in items_with_more_colls.keys():
    #     # Prepare request URL - replace `{item_uuid}` with current `item_with_more_coll_uuid`
    #     request_url = item2collection_url.replace('{item_uuid}', item_with_more_coll_uuid)
    #
    #     # Prepare request body which should looks like this:
    #     # `"https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_1}" + \n
    #     # "https://localhost:8080/spring-rest/api/core/collections/{collection_uuid_2}"
    #     request_body = []
    #     collection_url = 'core/collections/'
    #     for collection_uuid in coll_2_item_dict[item_with_more_coll_uuid]:
    #         request_body.append(API_URL + collection_url + collection_uuid)
    #
    #     do_api_post(request_url, {}, request_body)
    #
    # # save item dict as json
    # if save_dict:
    #     save_dict_as_json(ITEM_DICT, item_id_dict)
    # statistics_val = (statistics_dict['item'][0], imported_item)
    # statistics_dict['item'] = statistics_val


    # Migrate item versions
    migrate_item_history(metadata_class, items_dict, item_id_dict, eperson_id_dict, item_handle_item_metadata_dict)
    check_sum(item_id_dict)
    logging.info("Item and Collection2item were successfully imported!")


def check_sum(item_id_dict):
    # Imported versionitems uuid
    clarin_7_historyversion_uuid = ["22a84a63-a989-4d2d-a21f-e90890b4bb1e","7f9d2f70-de59-4509-8023-2dae18724c6a","20017ca9-7508-47b1-a3a4-db8374329d6d","519e8c61-9d0e-4a85-8070-632d15e685e3","49f21432-5fb9-4a3e-a03d-f42177c56128","17a54e55-b70c-42fd-b2f4-686624ac9de7","b23d80d4-6192-4cea-b524-fa6988a47b4c","d347714e-4914-453c-8147-b9079ec4094c","bfe89614-fac0-4074-ac71-dd708dc64860","4065581d-0ffd-4068-9035-d8dc78870d1d","7b40190f-ab04-439e-8096-3b91e4b5cd9c","4dadb421-ae3e-4185-bef2-242b07a9d46c","3c057693-eebd-48e5-9c83-3d7d767fc71e","8af23032-245c-41a5-90f9-32be22e4a5e8","7f4eec80-fda7-4196-be75-4338ae06b751","5945011d-9d98-468e-92b0-27d5e5316f5f","37a9c83f-1779-426a-ae53-167133b4b82d","e4b90615-a652-43f3-abec-d74efbbf69c3","22a27441-1013-4952-82bf-226e0057705b","ce72f95a-fb82-4ec4-be42-274e51e24504","d908606d-edb9-49cf-80c5-ac5e141e31e8","9c02d894-d7fb-4bba-8449-f510fe9a9a67","3a564ebb-f0bd-4e7a-bd07-89ef1b907b1a","16bf3ae7-94f2-491a-a12c-6052410a0dea","363dffad-8a8e-413a-a5b0-ceedfd615c2d","fcb8e8d6-6da9-479b-9f6a-c6cff09fc728","21a1b6db-85f1-4a71-8b12-376be919640b","0fbddf6e-df32-465d-8993-d21f06050cb7","7460aeb5-4e93-406a-b64a-416e7b90c836","fce24186-772b-4f89-bfc6-316180ffc59a","dba5d2cf-fb08-4da2-b2ca-36d582806e4e","8903c853-cb7a-4cb2-8996-3045098218f5","38e2e7b3-5512-42dc-9a7a-d350007cd34e","0fcbb6d6-8968-42bd-bd24-18a944438dae","70c987a2-af39-415e-8584-dbc074f4fb25","ca49b134-e1e6-4749-94fe-46ee9f21cfbd","61b3f3cb-981c-4e6c-a33c-7a32e4dd8187","36c9c124-dcf4-456c-be31-98d64fdb20ac","5b929894-ad2c-4236-99ec-efb5edd2d865","5bc1312b-b39d-4de7-ad4f-e995dd53ea31","4ebd3ba9-1b88-4dae-bae4-af91aff5f070","e0078161-1e60-461d-9c2a-ecbc5578ecbe","566de6fd-3946-4d40-830e-e867f6dd8957","9d88595e-fa77-4d46-8d88-fbd4f68e6cf7","3d777fa4-dea6-44b2-8d3a-ca1c513923b3","fa9bd6d8-8760-4b02-a289-e99ac9ffbe56","f270750a-ea5a-4175-a9db-dd83ec5bf42f","28a1f108-5312-4206-beb1-665d9d765214","ee1024a8-3a08-4f6b-bc10-449c034397ae","44e3018a-b7c3-43eb-97c6-1b4a4e5766cf","1061994c-73b6-43df-be71-4ff0126064b1","405026f7-1f2a-4523-b37f-7406a50027d3","3b3d87d5-7984-4bb8-a8ef-39968a83a4eb","49a41227-0584-40ac-9be3-69f1d1b098c7","85ea5f8d-0902-4748-8edf-b29b7e19de76","df10dfa5-d795-4b0e-a8a6-668d11a1b1bd","3e2e0605-b7b3-49d3-9246-a36d9bcddce7","0e38b0cb-be41-4b84-9214-df8ab3a6d621","5deb3fbf-83e6-454d-b8be-a620ae9dc11f","acd39d81-d96b-42b8-8861-38764956ffa2","5c2292ab-d5f8-4dee-b894-3fe92c616a32","986b977f-8086-438d-8b99-5689dd7ca22d","adeba648-ac55-4d3e-af16-91297ee63092","cc8330d4-d965-4d51-9d6d-22911270784b","3790f5eb-5643-4bae-aadf-1ff47133b62d","d37f8b54-d628-42c8-a6f5-9e25570c6165","b047f313-6f4a-4bd3-bccd-63191d7ba56f","ef13ff44-5877-43ba-b530-f408904429cd","04663554-720e-4461-bb15-d5134196e49a","df2c214d-2912-4240-a9f5-6e0f56e112c6","130aff07-2cbb-4336-8599-b66734fd1c62","7613f672-c173-4654-aeb4-f2d96aba4055","9fe99953-e5ab-4d45-8f91-63007a2dbeac","6b999ac4-65c0-456d-9940-3d0fa2f53825","f62633b8-d2b8-4bfe-869d-f8441f2b4ff4","fadb687e-0ec0-4bea-b359-6d85448d6564","92926ed7-8128-45ec-a2d0-5b5dc7bbebe1","af3c94ba-991a-411d-b080-c7022980d680","da4dd2ee-dd6b-4907-81be-26bab5486aaf","211d37b8-3a7f-4c8c-bb94-8b8f683b500e","2d0dba6c-fc3d-4f8d-9120-554ff6f527ef","a3afb575-90cb-43d5-9720-03400ee05773","88c76579-b149-43fe-b377-e3b791b23f78","52379558-4619-428f-93c3-22fba81b8757","822b2765-403f-443e-81eb-e569b6da3b38","a358d95d-04ec-4bc9-89af-1294f4e5b9b2","f3296fb6-33b7-4441-96c7-68b985ed099d","54525ef2-814b-4d5d-ab2c-7a84aad29c9d","ffc03f3b-7804-46bf-803c-3cc6a58b54f6","f3186a46-9aca-4ed9-834c-0dff732bb578","40332f31-c7a2-4b7e-b427-8b3579b60aaa","75379efc-c97b-4255-8e5b-cca4b5b237b5","4cafc81a-eeaf-41fe-a275-a2b3185f5f6d","ebc33834-1b4d-42cd-9489-0e276b9d4701","3957d0a2-bca1-459f-98a7-a961f8e5ad74","e964c2d4-9c0e-405a-a5b5-0be5a29c8f46","c6393d82-52e8-4579-abc8-cc5efabd1692","7bf3e38a-a356-4afe-8662-98a75e22a19b","bb80cafd-b6cc-43e2-9026-4755c8f9e900","028557fc-6d03-41b3-a966-f51b6fc21856","f713631b-6b59-4c11-a489-730c00aad7ad","63e84cfb-276e-4c86-a217-97ab4a0277d6","6888b395-da9b-44ee-944c-c96d947db992","9f499a33-2385-4027-9bcd-c062d4ce65ce","420cf3bd-f79a-49b6-93ff-5e553385f5c3","5fb018f6-48a7-4452-8db5-b2d1aca39dfb","625f70ea-717f-46bc-9c13-9c5443f6c8d6","15e546a8-2c99-40a8-a985-525cae376be2","9c8e7bfa-9d87-410e-a819-e2ead60f47c5","3c415670-884e-4844-8368-b3f2189dd2ad","66bc32e7-2532-4270-8e12-8de8791cbeb9","405dee10-a583-4d42-9e0b-f16393fb43c6","6c99e60e-f3cf-4423-b06d-bd2d0a902b63","5f9115e5-a492-4b01-86c4-c2c18a3ee578","494a5fe0-2487-42d5-a5f8-0729a6dc0e38","5e33e53e-37b8-4953-8b7d-f7ec761dcdea","1c4803aa-fd8e-4f34-bd52-41e008808125","cc148d1a-f934-446f-923f-c45a6042ffee","5ca2005a-b1e6-43a8-92f2-f4a56b53585b","8457f621-9301-497f-a993-0ec1998cd92f","54f4e03e-5503-4c02-9a69-380dc781ff7b","dff2a40c-38fd-4655-8f56-22a407fde67b","d825e5a0-f8b9-4fca-8de5-bb257c069c02","42723dad-ddf6-4169-a13c-1d25b6629ecf","2ecd0131-f3cc-40bb-82c3-3d467babb3b4","72bbcd85-cffc-4909-bf5e-322dbb0e1c3f","c981f3b5-06ab-4edb-acb9-c61b5c75668d","cd1a491d-3f38-4a0f-8a70-ab0ac852ab74","753d5ece-5653-4ed5-8087-b48b24d92cb9","2340c3d7-4db5-476b-951a-83aa9e1bf0c7","ce79bef1-b0c4-4b4e-a2de-e95e97f50382","c159ab76-3d0d-4b3e-99fb-4a60a3a487da","afbe1b37-0a45-404d-82da-a4ad55407af0","48d64b50-6ed6-4c47-aab9-9bbcc4306672","e707f4a2-c561-446e-b3ab-f1885fa0e2bc","0ab6d795-4ca9-4f12-a01b-f622e8c656b7","e2a42281-fd21-45f5-811f-47ff2adfd899","89c9a597-005c-46db-8c04-922c7a330fb0","7f41ab41-5449-4c93-860c-2e2a806d7afd","74d08f1b-54ca-4940-a3d2-e86a2ed6b69d","6a166898-dc51-48df-9a70-f430ee8e53b7","f6d99d39-add7-49a4-8353-1923446e11a7","dc0f7a92-c807-4fc2-8bca-da07ff145efd","5dd525bc-b375-4859-a6e2-6708280d6e8a","4c31158a-4add-48ee-9386-c64937421b8b","a8107ac2-0751-493a-9155-807a1dd2b7d1","9687df55-c294-49a0-804b-223df94f96d0","4215bee3-3ad5-4c8e-928d-75c1296b911c","de179eb6-8ba3-403a-afc4-1bab5893ee61","bb8bb4a9-624f-4f2f-8f18-e96ae19c7b8e","94e183dd-4727-4418-8741-4b23e54d1b9b","c6f45a64-4fcd-48c0-a543-262dfb973be3","d6544b86-b9e8-4d16-ac43-64235b41c21c","d4488717-f841-409c-8c4a-944b521eb71b","9f14cf3b-0ff1-4d72-bb7d-c3984f5c2dca","51f0f6c1-b3b2-418c-9df4-480444f54ce8","9c2cf172-4e09-429b-a3a6-1ceff9f0cac7","ad701a81-522b-4bb6-95e4-56fce820dfb0","b3c0069e-25a2-4df7-891e-c8b4e6fa883c","1f06a69a-b893-4de7-b3c7-c71e9dc714e4","bae40618-3b1e-4de1-b233-3263d2074e94","b454dc22-7f99-40b6-b928-7062f835037f","ba960929-6ac5-47d0-bbf3-51b524459c21","4d58d493-92d0-424a-be12-b61f30207b24","bc55071a-003a-4a47-8fae-4b30b1457504","20179d2e-9cea-4d6b-8fa8-607fa6ebd930","ec000e80-3aa3-48df-bc0f-905026d003fc","b4388366-89d9-4c35-bba8-46066962b549","e497e831-89b5-4ba2-90f0-fb97622276e8","69c76413-74c1-404f-a08e-4ae1a3cad51e","364e5078-ac70-4e18-ba47-07a84bdf1240","b24b98bc-10b7-4aee-8973-79615a2fe53f","1b7fafa2-19af-4cf1-95dd-40d52ff26b15","81f06694-d88d-45f2-8115-fe4318eddcff","b864cf4d-8db3-48e1-a994-a09882cc4e4a","5fdf2ac9-02c6-4208-830b-e00d49888201","b382cb30-bbf2-4441-b37e-c2bebd1de5ac","4a3c4bc8-aedb-4293-bcd1-15564f9ceb2e","0e38b0cb-be41-4b84-9214-df8ab3a6d621","5deb3fbf-83e6-454d-b8be-a620ae9dc11f","acd39d81-d96b-42b8-8861-38764956ffa2","5c2292ab-d5f8-4dee-b894-3fe92c616a32","d6d1e06c-ba09-4cd1-aa3a-3d2de4f83981","e39a50b3-c597-4efc-9b56-f71238800319","42dd167d-39f9-472a-8a94-2ae66c8fa087","cd1a491d-3f38-4a0f-8a70-ab0ac852ab74","753d5ece-5653-4ed5-8087-b48b24d92cb9","fa2228ba-a1e8-40a1-8d25-57db127335fa","0ab6d795-4ca9-4f12-a01b-f622e8c656b7","e2a42281-fd21-45f5-811f-47ff2adfd899","c1c2f588-8d31-4299-a5a3-586715c879b8","323fca5e-49b2-4619-bda9-20c4c7f6f762","74e00b47-6498-46cc-b55b-45f6e682da83","cb77b578-c09f-44ea-af8a-a34e2b7cbd5c","047347d3-db74-4d36-9277-27936bdc13b3","85d3c19a-44c9-4c59-961d-68c5a6087aac","0c405059-ff55-4fb9-9e6d-99c91e5931b2","356e6c4f-25de-4087-bf2e-a25505a62679","8ccb5afd-9f59-4a05-950d-df09008f9200","218e2484-a75d-44e9-883a-5e99333c1af6","b4abf1c7-42c6-465f-8f21-bd7c5f23868d","8723e755-964a-429a-93ac-b13508749a83","12a730e8-2fe0-460d-b6b3-12e36727b94f","6888b395-da9b-44ee-944c-c96d947db992","420cf3bd-f79a-49b6-93ff-5e553385f5c3","5fb018f6-48a7-4452-8db5-b2d1aca39dfb","78f0cf1b-dd7e-493f-875f-2036aa8a6a26","e5f69642-9f1b-42fb-be46-89adb8f67a1e","26365761-5fd8-4d9f-a61c-21f7e7ee7bc4","3076e385-ab80-4573-a667-d75e3d06f621","083f3414-e74c-4c4c-a0bd-9e9990a1d9fe","0c865d3c-b5ff-4d38-8cb2-80bdac11db66","ff0102e7-809a-4c75-80ed-24054962b4f3","c67b420e-e268-41b8-8b9c-1db5ad1c7d86","118a1aee-c372-4ea1-8e6e-31a3a3ee2ef9","e483cb14-7721-4965-b9eb-7ede072cb9e1","a74a2574-597a-42db-b4da-797392e1eb88","be9179b8-11fb-4372-bd12-ed0fbb849dd8","02cd1325-1cfc-4e22-9f34-b22a3d8f3c1f","3213f34e-9d4c-4dcb-98d7-0749260e3143","b4475163-49af-4f65-afe3-23294a99b3b7","c6fb3749-08e5-40a6-ae3a-229d83f3b31d","9d3ab848-c12f-47fa-b0a6-13b6c5879b75","fdfe8dea-52d0-457e-923f-f480ad0d90d5","fb7ec959-9d0f-4837-88fd-ae8325d117c6","2bfc7d27-f7c4-4176-bf4a-212b5a57b781","27da20d2-9679-4a00-aaa0-3ecbd7ab9ff1","42e08282-045d-4048-97e2-408d8a566983","6f181433-84db-447f-ba5f-e4ce33bc1c9f","622ecda5-d066-47ba-91b6-9602cb4ab402","0878c648-0bd1-4cad-9147-6f0d3314589c","16018d0f-033e-4c35-9850-ea2d33924c86","9454b981-5f1a-4f29-aefc-c1c2ad268d56","3f11a852-b979-48eb-9c03-0a97b1446829","03b77f77-5dd2-4679-b210-f21f1e0ca968","e4b90615-a652-43f3-abec-d74efbbf69c3","22a27441-1013-4952-82bf-226e0057705b","ce72f95a-fb82-4ec4-be42-274e51e24504","d908606d-edb9-49cf-80c5-ac5e141e31e8","9c02d894-d7fb-4bba-8449-f510fe9a9a67","3a564ebb-f0bd-4e7a-bd07-89ef1b907b1a","16bf3ae7-94f2-491a-a12c-6052410a0dea","363dffad-8a8e-413a-a5b0-ceedfd615c2d","fcb8e8d6-6da9-479b-9f6a-c6cff09fc728","21a1b6db-85f1-4a71-8b12-376be919640b","0fbddf6e-df32-465d-8993-d21f06050cb7","7460aeb5-4e93-406a-b64a-416e7b90c836","fce24186-772b-4f89-bfc6-316180ffc59a","5fdf2ac9-02c6-4208-830b-e00d49888201","b382cb30-bbf2-4441-b37e-c2bebd1de5ac","4a3c4bc8-aedb-4293-bcd1-15564f9ceb2e","72b21c0f-7d84-4c09-bd37-9aa2907d59be"]

    clarin_5_history_ids = [3526,2877,2913,3278,3161,3279,3279,2918,244,216,2920,200,3110,3286,3200,3287,3225,255,2956,3321,3513,2963,2965,2969,2913,3680,3528,3308,3180,3318,3238,3321,3220,3199,3334,3235,3340,3234,3335,14,28,95,152,268,256,203,171,95,32,222,200,216,212,3487,237,47,3401,2845,2842,2842,266,3492,2855,6,2863,2845,3350,3351,3286,3287,3488,3442,3714,3352,3727,3414,3487,3387,3495,3530,3318,3733,3204,3353,3097,3034,3363,3385,3352,3496,3492,3340,3387,3389,3353,3294,3394,3453,3485,3505,3388,2835,3424,3182,2875,3265,3430,3265,3431,3430,3426,3439,3371,3438,3416,3517,3442,3385,3427,3334,3335,3428,3518,3488,3435,3244,3458,3459,3460,3461,3462,3448,3445,3446,3447,3443,3464,3320,3456,3389,3516,3526,3528,3474,3435,3477,3477,3478,3097,4541,3683,3485,136,3036,3279,4542,4540,3578,3209,3209,3581,3479,3583,3477,3201,3500,3595,3589,4537,3683,3495,3636,3682,3566,3518,4583,4561,3150,3185,3682,4534,4534,4534,3693,4556,3104,3515,4537,2439,4541,4617,3472,4627,3213,4620,3636,3714,4625,3642,4652,4656,4651,3099,4654,4645,4664,4688,4667,4646,3583,4689,3575,4557,4740,4738,3063,4662,4745,4741,4627,4828,4823,4747,4583,4840,3431,4901,3722,122,2886,2984,3020,3064,151,3034,3046,2984,3035,241,3065,224,45,3085,3024,238,2972,3094,3046,3094,3068,3035,3099,3148,3161,3020,3163,3164,3170,3171,180,236,3085,3201,3179,8,3087,3220,3232,2967,3234,3235,3088,3237,3268,3255,3109,2858,3164,3238,118,200,200,3110,3264,3211]
    clarin_5_ids_to_uuid = []
    # Convert item_id to uuid
    for clarin_5_id in clarin_5_history_ids:
        clarin_5_ids_to_uuid.append(item_id_dict[clarin_5_id])

    print('len(clarin_5_history_ids): ' + str(len(clarin_5_history_ids)))
    print('len(clarin_5_ids_to_uuid): ' + str(len(clarin_5_ids_to_uuid)))

    # Check if the clarin_5_uuid is in the clarin_7_historyversion_uuid
    for clarin_7_uuid in clarin_7_historyversion_uuid:
        # clarin_5_uuid = item_id_dict[clarin_5_id]
        if clarin_7_uuid not in clarin_5_ids_to_uuid:
            print(f'Item with {clarin_7_uuid} is not imported')


def migrate_item_history(metadata_class,
                         items_dict,
                         item_id_dict,
                         eperson_id_dict,
                         item_handle_item_metadata_dict):
    logging.info("Going to migrate versions of all items.")
    c7_dspace = connect_to_db(database=const.CLARIN_DSPACE_7_NAME,
                              host=const.CLARIN_DSPACE_7_HOST,
                              port=const.CLARIN_DSPACE_7_PORT,
                              user=const.CLARIN_DSPACE_7_USER,
                              password=const.CLARIN_DSPACE_7_PASSWORD)

    cursor_c7_dspace = c7_dspace.cursor()
    admin_uuid = get_admin_uuid(cursor_c7_dspace)

    # 1. Create sequence of item versions
    #     Store Items for which the version was created in some list - do not create a version for the same item

    # Test only for one item TODO run it in `for`
    # Get item_id by uuid
    # item_id = 3094
    processed_items_id = []
    for item in items_dict.values():
        # if item['item_id'] != item_id:
        #     continue
        # Do not process withdrawn items.
        item_id = item['item_id']
        # Process only the items which has `dc.relation.replaces` or `dc.relation.isreplacedby` metadata
        # Do not process withdrawn items because the new location of the withdrawn item is stored in the same
        # metadatafield as the new version of the item
        # if item['withdrawn']:
        #     continue
        # Do not process versions of the item that have already been processed.
        if item_id in processed_items_id:
            continue

        # This sequence contains handles of all versions of the Item ordered from the first version to the latest one
        # item_version_sequence = ['http://hdl.handle.net/11234/1-1464', 'http://hdl.handle.net/11234/LRT-1478', 'http://hdl.handle.net/11234/1-1548', 'http://hdl.handle.net/11234/1-1699', 'http://hdl.handle.net/11234/1-1827', 'http://hdl.handle.net/11234/1-1983', 'http://hdl.handle.net/11234/1-2515', 'http://hdl.handle.net/11234/1-2837', 'http://hdl.handle.net/11234/1-2895', 'http://hdl.handle.net/11234/1-2988', 'http://hdl.handle.net/11234/1-3105', 'http://hdl.handle.net/11234/1-3226', 'http://hdl.handle.net/11234/1-3424', 'http://hdl.handle.net/11234/1-3683', 'http://hdl.handle.net/11234/1-3687', 'http://hdl.handle.net/11234/1-4611', 'http://hdl.handle.net/11234/1-4758', 'http://hdl.handle.net/11234/1-4923']
        item_version_sequence = get_item_version_sequence(item_id, items_dict, metadata_class, item_handle_item_metadata_dict)

        # Do not process item which does not have any version
        if item_version_sequence is None:
            continue

        logging.info(f'Going to process all versions for the item with ID: {item_id}')
        # All versions of this Item is going to be processed
        processed_items_id.append(item_id)

        # Insert data into `versionhistory`
        versionhistory_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionhistory', 'versionhistory_id') + 1
        cursor_c7_dspace.execute("INSERT INTO versionhistory(versionhistory_id) VALUES (" +
                                 str(versionhistory_new_id) + ");")
        c7_dspace.commit()

        for item_blabla in item_handle_item_metadata_dict.keys():
            if item_handle_item_metadata_dict[item_blabla] is None:
                print(f'item_blabla {item_blabla}')

        # Insert data into `versionitem` with `versionhistory` id
        versionitem_new_id = get_last_id_from_table(cursor_c7_dspace, 'versionitem', 'versionitem_id') + 1
        for index, item_version_handle in enumerate(item_version_sequence, 1):
            # If the item is withdrawn the new version could be stored in our repo or in another. Do import the version
            # only if the item is stored in our repo.
            if item_version_handle not in item_handle_item_metadata_dict:
                current_item = items_dict[item_id]
                # Ignore withdrawn item fetch using `...replace..` metadata fields
                if current_item['withdrawn']:
                    logging.info(f'The item with handle: {item_version_handle} cannot be migrated because'
                                 f' it is stored in another repository.')
                    continue

            # Get the handle of the x version of the Item
            item_handle_id_dict = item_handle_item_metadata_dict[item_version_handle]
            # Get item_id using the handle
            item_id = item_handle_id_dict['item_id']
            # # Get current item


            # Get the uuid of the item using the item_id
            item_uuid = item_id_dict[item_id]
            # timestamp is required column in the database
            timestamp = datetime.datetime.now()
            cursor_c7_dspace.execute(f'INSERT INTO public.versionitem(versionitem_id, version_number, version_date, version_summary, versionhistory_id, eperson_id, item_id) VALUES ('
                                     f'{versionitem_new_id}, '
                                     f'{index}, '
                                     f'\'{timestamp}\', '
                                     f'\'\', '
                                     f'{versionhistory_new_id}, '
                                     f'\'{admin_uuid}\', '
                                     f'\'{item_uuid}\');')
            versionitem_new_id += 1
            processed_items_id.append(item_id)
        c7_dspace.commit()

    logging.info("Processing of the item versions was successful!")
    logging.info(f'Count of the items which has connected any version is: {len(processed_items_id)}')


def get_admin_uuid(cursor):
    # Execute a SQL query to retrieve the last record's ID (assuming 'your_table' is the name of your table)
    cursor.execute(f'SELECT uuid FROM eperson WHERE email like \'{const.user}\'')

    # Fetch the result
    eperson_uuid = cursor.fetchone()

    uuid = ''
    # Check if there is a result and extract the ID
    if eperson_uuid:
        uuid = eperson_uuid[0]
    else:
        logging.error("No eperson records in the table.")

    return uuid
def get_last_id_from_table(cursor, table_name, id_column):
    # Execute a SQL query to retrieve the last record's ID (assuming 'your_table' is the name of your table)
    cursor.execute("SELECT " + id_column + " FROM " + table_name + " ORDER BY " + id_column + " DESC LIMIT 1")

    # Fetch the result
    last_record_id = cursor.fetchone()

    # Default value - the table is empty
    last_id = 1
    # Check if there is a result and extract the ID
    if last_record_id:
        last_id = last_record_id[0]
    else:
        logging.error("No records in the table.")

    # Close the cursor and the database connection
    return last_id


def get_item_version_sequence(item_id,
                              items_dict,
                              metadata_class,
                              item_handle_item_metadata_dict):
    # True = previous; False = newer
    # isreplacedby
    # ['hdl1', 'hdl2', ['hdl3', 'hdl4'], [['hdl5', 'hdl6']]]
    # newer_versions = get_item_versions(item_id, metadata_class, item_handle_item_metadata_dict,
    #                                       True)
    one_more_version_replaced = ['http://hdl.handle.net/11858/00-097C-0000-0006-DB11-8',
                                 ['http://hdl.handle.net/11858/00-097C-0000-0006-53765',
                                  'http://hdl.handle.net/11858/00-097C-0000-0008-E130-A']]
    newer_versions = get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, True, [])
    # replaces
    previous_versions = get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, False, [])
    # reverse
    previous_versions = previous_versions[::-1]
    # previous_versions.reverse()
    # print(f'newer: {str(newer_versions)}')

    # If this item does not have any version return a None
    # if len(newer_versions) == 0 and len(previous_versions) == 0:
    #     return None

    current_item_handle = getMetadataValues(item_id, metadata_class, DC_IDENTIFIER_URI_ID)
    # Some items do not have handle, it has handle in the Item with different ID.
    if current_item_handle is None or len(current_item_handle) == 0:
        return None

    if len(current_item_handle) > 1:
        logging.error('Pay attention! This Item has two handles: ' + str(current_item_handle))
    # return previous_versions + [current_item_handle[0]] + newer_versions

# def get_item_metadata(item_id, metadata_class, metadata_field):


def get_item_versions(item_id, items_dict, metadata_class, item_handle_item_metadata_dict, previous_or_newer: bool,
                      versions):
    # True = previous; False = newer
    # Get previous version - fetch metadata value from `dc.relation.replaces`
    # Get newer version - fethc metadata value from `dc.relation.isreplaced.by`
    metadata_field_id = DC_RELATION_REPLACES_ID
    if previous_or_newer:
        metadata_field_id = DC_RELATION_ISREPLACEDBY_ID

    list_of_version = []
    #
    # list1 = ['hdl1', 'hdl2', 'hdl3']
    # list2 = ['hdl4', 'hdl5']
    # list3 = ['hdl6']
    #
    # more_versions_replaced = ['http://hdl.handle.net/11858/00-097C-0000-0006-DB11-8', 'http://hdl.handle.net/11858/00-097C-0000-0008-E130-A']
    # one_version_replaced = ['http://hdl.handle.net/11858/00-097C-0000-0006-DB11-8']

    # get metadata of the item
    version_handles = getMetadataValues(item_id, metadata_class, metadata_field_id)
    if version_handles is None or len(version_handles) == 0:
        # There are no more version connections in the item - return current list of versions
        return versions

    # If item is withdrawn and has more replacements add this info into `isreplacedby`
    handles_items_in_another_repo = []
    # get metadata values of replaced or isreplacedby
    for version_handle in version_handles:
        if version_handle not in item_handle_item_metadata_dict:
            # Check if current item is withdrawn
            item = items_dict[item_id]
            if item['withdrawn']:
                # The item is withdrawn and stored in another repository
                handles_items_in_another_repo.append(version_handle)
                continue
            else:
                logging.error(f'The item with handle: {version_handle} has not been imported!')
                continue
        item_id = item_handle_item_metadata_dict[version_handle]['item_id']
        versions.append(version_handle)
        fetched_version_handles = getMetadataValues(item_id, metadata_class, metadata_field_id)

        # 1 - it is None or null
        if fetched_version_handles is None or len(fetched_version_handles) == 0:
            break
        # 2 - list with 1 value
        elif len(fetched_version_handles) == 1:
            versions.append(fetched_version_handles[0])
        # 3 - list with more values - get all handles
        else:
            list_of_version.append(get_item_versions(item_id, metadata_class, item_handle_item_metadata_dict,
                                                     previous_or_newer, versions))
        #
        #
        # if fetched_version_handles:
        #     # If the item is a list, recursively flatten it and add its elements
        #     list_of_version.append(get_item_versions(item, metadata_field_id))
        # else:
        #     # If the item is not a list, add it to the result list
        #     list_of_version.append(item)

    if handles_items_in_another_repo:
        return versions + [handles_items_in_another_repo]
    return versions


    # current_item_id = item_id
    # # current_version is handle of previous or newer item
    # current_versions = getMetadataValues(current_item_id, metadata_class, metadata_field_id)
    # if current_versions is                                             not None and len(current_versions) == 1:
    #     current_version = current_versions[0]
    #     while current_version is not None:
    #         logging.info(f'current_version: {current_version}')
    #         list_of_version.append(current_version)
    #         try:
    #             current_item_id = item_handle_item_metadata_dict[current_version]['item_id']
    #             # Code that might raise an exception
    #         except Exception as e:
    #             # This exception occurs when the item is withdrawn and it is not added into `item_handle_item_metadata_dict`
    #             # because withdrawn items doesn't have `resource_id` metadata in the `metadatavalue` table.
    #             logging.info(f'Warning: Versions of the Item with handle {current_version} cannot be processed because'
    #                          f' the Item is probably withdrawn.')
    #             # Do not continue looking for versions of the item that have been withdrawn.
    #             current_version = None
    #             continue
    #         # current_item_id = item_handle_item_metadata_dict[current_version]['item_id']
    #         current_versions = getMetadataValues(current_item_id, metadata_class, metadata_field_id)
    #         if len(current_versions) == 1:
    #             current_version = current_versions[0]
    #         elif current_versions is not None and len(current_versions) != 0:
    #             print('Current version has more versions: ' + str(current_versions))
    #         else:
    #             current_version = None
    # elif current_versions is not None and len(current_versions) != 0:
    #     print('Current version has more versions: ' + str(current_versions))

        # if current_item_id == item_handle_item_metadata_dict[current_version][metadata_field]:
        #     current_version = None
        #     continue
        # Get item_id by the handle
        # current_item_id = item_handle_item_metadata_dict[current_version]['item_id']

        # OLD
        # if metadata_field not in item_handle_item_metadata_dict[current_version].keys():
        #     current_version = None
        #     continue
        # current_version = item_handle_item_metadata_dict[current_version][metadata_field]

    return list_of_version


def get_item_id_by_handle(handle, items_dict, metadata_class):
    return 3084


def getMetadataValues(item_id, metadata_class, metadata_field_id):
    if item_id is None:
        return None

    # 2 = resource_type = Item
    try:
        # It returns a dict of metadata_values
        all_metadata_values = metadata_class.metadatavalue_dict[(2, item_id)]
        # Wanted metadata values
        wanted_metadata_values = []
        # because metadata value are stored in the list
        for metadata_value in all_metadata_values:
            if metadata_value['metadata_field_id'] != metadata_field_id:
                continue
            wanted_metadata_values.append(metadata_value['text_value'])
        return wanted_metadata_values
        # if metadata_field_id not in all_metadata_values:
        #     return None
    except Exception as e:
        print('Heee')


    # metadata_value_list = metadata_values[metadata_field_id]
    # if metadata_value_list:
    #     return metadata_value_list[0]['value']
    return None

# def getMetadataValues(metadata_values):
#     return metadata_values['dc.relation.isreplacedby']


def import_workspaceitem(item,
                         owning_collectin_id,
                         multiple_titles,
                         published_before,
                         multiple_files,
                         stagereached,
                         page_reached,
                         metadata_class,
                         handle_class,
                         workspaceitem_id_dict,
                         item_id_dict,
                         collection_id_dict,
                         eperson_id_dict):
    """
    Auxiliary method for import item.
    Import data into database.
    Mapped tables: workspaceitem, metadata, handle
    """
    workspaceitem_url = 'clarin/import/workspaceitem'
    workspaceitem_json_p = {
        'discoverable': item['discoverable'],
        'inArchive': item['in_archive'],
        'lastModified': item['last_modified'],
        'withdrawn': item['withdrawn']
    }
    metadatavalue_item_dict = metadata_class.get_metadata_value(2, item['item_id'])
    if metadatavalue_item_dict is not None:
        workspaceitem_json_p['metadata'] = metadatavalue_item_dict
    handle_workspaceitem = handle_class.get_handle(2, item['item_id'])
    if handle_workspaceitem is not None:
        workspaceitem_json_p['handle'] = handle_workspaceitem
    # the params are workspaceitem attributes
    params = {
        'owningCollection': collection_id_dict[owning_collectin_id],
        'multipleTitles': multiple_titles,
        'publishedBefore': published_before,
        'multipleFiles': multiple_files, 'stageReached': stagereached,
        'pageReached': page_reached,
        'epersonUUID': eperson_id_dict[item['submitter_id']]
    }
    try:
        response = do_api_post(workspaceitem_url, params, workspaceitem_json_p)
        workspaceitem_id = convert_response_to_json(response)['id']
        workspaceitem_id_dict[item['item_id']] = workspaceitem_id
        item_url = API_URL + 'clarin/import/' + str(workspaceitem_id) + "/item"
        try:
            response = rest_proxy.d.api_get(item_url, None, None)
            item_id_dict[item['item_id']] = convert_response_to_json(response)['id']
        except Exception as e:
            logging.error('POST request ' + item_url +
                          ' failed. Exception: ' + str(e))
    except Exception as e:
        logging.error('POST request ' + workspaceitem_url + ' for id: ' +
                      str(item['item_id']) +
                      ' failed. Exception: ' + str(e))

