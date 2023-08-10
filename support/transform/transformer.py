"""
This scripts transforms web metadata to .json form that can be used for importing
through dspace_proxy from support.dspace_proxy.rest_proxy.

It expects lines copied from web, e.g.
https://lindat.mff.cuni.cz/repository/xmlui/handle/11858/
00-097C-0000-0001-CC1E-B?show=full
in file `website_copy.txt` and emits output in `out.json`

It adds some useful data (name, withdrawn, in_archive...), but please note it does not
filter metadata. Some are not importable to other installations of dspace and they will
have to be manually filtered.
"""

import json

x = open("website_copy.txt", encoding="utf-8")


# in_feed = json.load(x)

class Crate:
    def __init__(self, md, vl):
        self.md = md
        self.vl = vl

    def __str__(self):
        return self.md + ":" + self.vl


value_list = []
for row in x.readlines():
    split = row.split("\t")
    dat = split[0].strip()
    val = split[1].strip()
    value_list.append(Crate(dat, val))
x.close()

metadata_list = {}
for val in value_list:
    if val.md not in metadata_list:
        metadata_list[val.md] = []
    spec = {"value": val.vl,
            "language": None,
            "authority": None,
            "confidence": -1,
            "place": 0
            }
    metadata_list[val.md].append(spec)

out_feed = {"name": metadata_list["dc.title"][0]["value"],
            "metadata": metadata_list, "inArchive": True,
            "discoverable": True,
            "withdrawn": False,
            "type": "item"}
out = open("out.json", "w+", encoding="utf-8")
out.write(json.dumps(out_feed, indent=4, ensure_ascii=False))
out.close()
