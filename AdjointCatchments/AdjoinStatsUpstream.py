import pandas
import os
from glob import glob
import json
import AdjoinUpdown as adj

jsons_dir = "../RegionalAdjointCatchmentCOMID_JSONs"
jsons = glob(os.path.join(jsons_dir, "*.json"))
all_regions_out = os.path.join(jsons_dir, 'all-regions-upstream_dict.json')
all_regions = {}
if not os.path.exists(all_regions_out):
    for js in jsons:
        print(js)
        f = open(js)
        cur = json.load(f)
        all_regions.update(cur)
    with open(all_regions_out, 'w') as f:
        json.dump(all_regions, f)
else:
    f = open(all_regions_out)
    all_regions = json.load(f)
print(all_regions['13082861'])

