import sys
import os
import logging
_logger = logging.getLogger("dspace")
_this_dir = os.path.dirname(os.path.abspath(__file__))

__all__ = [
    "rest",
]

path_to_dspace_lib = os.path.join(_this_dir, "../../libs/dspace-rest-python")
sys.path.insert(0, path_to_dspace_lib)

from ._rest import rest
