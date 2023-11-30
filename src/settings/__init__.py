__all__ = [
    #    "dspace",
    "env",
]

env = {

}

# from ._dspace import
from ._cache import settings  # noqa
env["cache"] = settings

from ._dspace import settings  # noqa
env["dspace"] = settings
