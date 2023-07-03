import logging

import var_declarations as var
from data_pump.bitstream import import_bitstream
from data_pump.bitstreamformatregistry import import_bitstreamformatregistry
from data_pump.bundle import import_bundle
from data_pump.collection import import_collection
from data_pump.community import import_community
from data_pump.user_metadata import import_user_metadata
from data_pump.eperson import import_eperson, import_group2eperson
from data_pump.epersongroup import import_epersongroup, import_group2group
from data_pump.handle import Handle
from data_pump.item import import_item
from data_pump.license import import_license
from data_pump.metadata import Metadata
from data_pump.registrationdata import import_registrationdata
from data_pump.tasklistitem import import_tasklistitem
from data_pump.user_registration import import_user_registration
from utils import read_json


def at_the_end_of_import(handle_class, statistics_dict):
    # write statistic about handles
    handle_json_a = read_json("handle.json")
    statistics_dict['handle'] = (len(handle_json_a), handle_class.get_imported_handle())
    # write statistic into log
    logging.info("Statistics:")
    for key, value in statistics_dict.items():
        logging.info(key + ": " + str(value[0]) +
                     " expected and imported " + str(value[1]))


if __name__ == "__main__":
    handle_class = Handle()
    metadata_class = Metadata(var.statistics_dict)

    logging.info("Data migration started!")
    import_community(metadata_class,
                     handle_class,
                     var.group_id_dict,
                     var.community_id_dict,
                     var.community2logo_dict,
                     var.statistics_dict)
    import_collection(metadata_class,
                      handle_class,
                      var.group_id_dict,
                      var.community_id_dict,
                      var.collection_id_dict,
                      var.collection2logo_dict,
                      var.statistics_dict)
    import_registrationdata(var.statistics_dict)
    import_epersongroup(metadata_class,
                        var.group_id_dict,
                        var.statistics_dict)
    import_group2group(var.group_id_dict, var.statistics_dict)
    import_eperson(metadata_class,
                   var.eperson_id_dict,
                   var.email2epersonId_dict,
                   var.statistics_dict)
    import_user_registration(var.email2epersonId_dict,
                             var.eperson_id_dict,
                             var.user_registration_id_dict,
                             var.statistics_dict)
    import_group2eperson(var.eperson_id_dict,
                         var.group_id_dict,
                         var.statistics_dict)
    import_license(var.eperson_id_dict, var.statistics_dict)
    import_item(metadata_class,
                handle_class,
                var.workflowitem_id_dict,
                var.item_id_dict,
                var.collection_id_dict,
                var.eperson_id_dict,
                var.statistics_dict)
    import_tasklistitem(var.workflowitem_id_dict,
                        var.eperson_id_dict,
                        var.statistics_dict)
    import_bitstreamformatregistry(var.bitstreamformat_id_dict,
                                   var.unknown_format_id_val,
                                   var.statistics_dict)
    import_bundle(metadata_class,
                  var.item_id_dict,
                  var.bundle_id_dict,
                  var.primaryBitstream_dict,
                  var.statistics_dict)
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
                     var.statistics_dict)
    import_user_metadata(var.bitstream_id_dict,
                         var.user_registration_id_dict,
                         var.statistics_dict)
    at_the_end_of_import(handle_class, var.statistics_dict)
    logging.info("Data migration is completed!")
