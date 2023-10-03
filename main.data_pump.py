import argparse
import sys
import logging

import data_pump.var_declarations as var
from data_pump.bitstream import import_bitstream
from data_pump.bitstreamformatregistry import import_bitstreamformatregistry
from data_pump.bundle import import_bundle
from data_pump.collection import import_collection
from data_pump.community import import_community
from data_pump.resourcepolicy import import_resource_policies, delete_all_resource_policy
from data_pump.user_metadata import import_user_metadata
from data_pump.eperson import import_eperson, import_group2eperson
from data_pump.epersongroup import import_epersongroup, import_group2group, \
    load_admin_anonymous_groups
from data_pump.handle import Handle
from data_pump.item import import_item
from data_pump.license import import_license
from data_pump.metadata import Metadata
from data_pump.registrationdata import import_registrationdata
from data_pump.tasklistitem import import_tasklistitem
from data_pump.user_registration import import_user_registration
from data_pump.utils import read_json, create_dict_from_json
from data_pump.sequences import migrate_sequences

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("root")


def at_the_end_of_import(handle_class_p, statistics_dict):
    # write statistic about handles
    handle_json_list = read_json("handle.json")
    statistics_dict['handle'] = (len(handle_json_list),
                                 handle_class_p.get_imported_handle())
    # write statistic into log
    _logger.info("Statistics:")
    for key, value in statistics_dict.items():
        # resourcepolicy has own style of statistics
        if key == 'resourcepolicy':
            string = ''
            for rpKey, rpValue in value.items():
                string += str(rpValue) + ' ' + rpKey + ', '
            _logger.info(key + ": " + string)
            continue
        _logger.info(key + ": " + str(value[0]) +
                     " expected and imported " + str(value[1]))


def load_data_into_dicts(eperson_json_name, user_registraion_json_name,
                           group_json_name, community_json_name, collection_json_name,
                           item_json_name, workspaceitem_json_name, workflow_json_name,
                           bitstreamformat_json_name, bundle_json_name,
                           bitstream_json_name, insert_data):
    if not insert_data:
        return
    var.eperson_id_dict = create_dict_from_json(eperson_json_name)
    var.user_registration_id_dict = create_dict_from_json(user_registraion_json_name)
    var.group_id_dict = create_dict_from_json(group_json_name)
    var.community_id_dict = create_dict_from_json(community_json_name)
    var.collection_id_dict = create_dict_from_json(collection_json_name)
    var.item_id_dict = create_dict_from_json(item_json_name)
    var.workspaceitem_id_dict = create_dict_from_json(workspaceitem_json_name)
    var.workflowitem_id_dict = create_dict_from_json(workflow_json_name)
    var.bitstreamformat_id_dict = create_dict_from_json(bitstreamformat_json_name)
    var.bundle_id_dict = create_dict_from_json(bundle_json_name)
    var.bitstream_id_dict = create_dict_from_json(bitstream_json_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload values into dictionaries')
    parser.add_argument('--load_dict_bool',
                        help='bool value if we load values into dict',
                        required=False, type=bool, default=False)
    parser.add_argument('--save_dict_bool',
                        help='bool value if we save dict values into jsons',
                        required=False, type=bool, default=False)
    args = parser.parse_args()

    # Is the email server really off?
    email_s_off = input("Please make sure your email server is turned off. "
                        "Otherwise unbearable amount of emails will be sent. "
                        "Is your EMAIL SERVER really OFF? (Y/N)")
    email_s_off = email_s_off.lower()
    # terminate the program
    if email_s_off not in ("y", "yes"):
        sys.exit()

    load_data_into_dicts("eperson_dict.json",
                           "user_registration_dict.json",
                           "epersongroup_dict.json",
                           "community_dict.json",
                           "collection_dict.json",
                           "item_dict.json",
                           "workspaceitem_dict.json",
                           "workflowitem_dict.json",
                           "bitstreamformatregistry_dict.json",
                           "bundle_dict.json",
                           "bitstream_dict.json",
                           args.load_dict_bool)
    handle_class = Handle()
    metadata_class = Metadata(var.statistics_dict, args.load_dict_bool)

    _logger.info("Data migration started!")
    # group Administrator and Anonymous already exist, load them
    load_admin_anonymous_groups(var.group_id_dict)
    import_community(metadata_class,
                     handle_class,
                     var.group_id_dict,
                     var.community_id_dict,
                     var.community2logo_dict,
                     var.statistics_dict,
                     args.save_dict_bool)
    import_collection(metadata_class,
                      handle_class,
                      var.group_id_dict,
                      var.community_id_dict,
                      var.collection_id_dict,
                      var.collection2logo_dict,
                      var.statistics_dict,
                      args.save_dict_bool)
    import_registrationdata(var.statistics_dict)
    import_epersongroup(metadata_class,
                        var.group_id_dict,
                        var.statistics_dict,
                        args.save_dict_bool)
    import_group2group(var.group_id_dict, var.statistics_dict)
    import_eperson(metadata_class,
                   var.eperson_id_dict,
                   var.email2epersonId_dict,
                   var.statistics_dict,
                   args.save_dict_bool)
    import_user_registration(var.email2epersonId_dict,
                             var.eperson_id_dict,
                             var.user_registration_id_dict,
                             var.statistics_dict,
                             args.save_dict_bool)
    import_group2eperson(var.eperson_id_dict,
                         var.group_id_dict,
                         var.statistics_dict)
    import_license(var.eperson_id_dict, var.statistics_dict, args.save_dict_bool)
    import_item(metadata_class,
                handle_class,
                var.workflowitem_id_dict,
                var.workspaceitem_id_dict,
                var.item_id_dict,
                var.collection_id_dict,
                var.eperson_id_dict,
                var.statistics_dict,
                args.save_dict_bool)
    import_tasklistitem(var.workflowitem_id_dict,
                        var.eperson_id_dict,
                        var.statistics_dict)
    var.unknown_format_id_val = import_bitstreamformatregistry(
        var.bitstreamformat_id_dict,
        var.unknown_format_id_val,
        var.statistics_dict,
        args.save_dict_bool)
    import_bundle(metadata_class,
                  var.item_id_dict,
                  var.bundle_id_dict,
                  var.primaryBitstream_dict,
                  var.statistics_dict,
                  args.save_dict_bool)
    import_bitstream(metadata_class,
                     var.bitstreamformat_id_dict,
                     var.primaryBitstream_dict,
                     var.bitstream2bundle_dict,
                     var.bundle_id_dict,
                     var.community2logo_dict,
                     var.collection2logo_dict,
                     var.bitstream_id_dict,
                     var.community_id_dict,
                     var.collection_id_dict,
                     var.unknown_format_id_val,
                     var.statistics_dict,
                     args.save_dict_bool)
    import_user_metadata(var.bitstream_id_dict,
                         var.user_registration_id_dict,
                         var.statistics_dict)
    # before importing of resource policies we have to delete all
    # created data
    delete_all_resource_policy()
    import_resource_policies(var.community_id_dict,
                             var.collection_id_dict,
                             var.item_id_dict,
                             var.bundle_id_dict,
                             var.bitstream_id_dict,
                             var.eperson_id_dict,
                             var.group_id_dict,
                             var.statistics_dict)
    # migrate sequences
    migrate_sequences()

    at_the_end_of_import(handle_class, var.statistics_dict)
    _logger.info("Data migration is completed!")
