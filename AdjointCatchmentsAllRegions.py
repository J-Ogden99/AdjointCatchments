import json
import os
import zipfile
import queue
from glob import glob
import geopandas as gpd
import time
import pandas as pd

# parse_dir = sys.argv[1]
# out_dir = sys.argv[2]
parse_dir = 'GEOGloWS-Delineation-Shapefiles'
out_dir = 'RegionalAdjointCatchmentJSONs'
catchment_zips = glob(os.path.join(parse_dir, '*catchment.zip'))
drainageline_zips = glob(os.path.join(parse_dir, '*drainageline.zip'))
# catchment_zips = glob(os.path.join(parse_dir, '*/*basins*.gpkg'))
# drainageline_zips = glob(os.path.join(parse_dir, '*/*streamnet*.shp'))
print(catchment_zips)
catch_dirs = []
drain_dirs = []
for catch, drain in zip(catchment_zips, drainageline_zips):
    catch_name, drain_name = os.path.basename(catch), os.path.basename(drain)
    catch_dir = os.path.join(parse_dir, catch_name[:-4])
    catch_dirs.append(catch_dir)
    if not os.path.exists(catch_dir):
        with zipfile.ZipFile(catch, 'r') as zip_ref:
            os.mkdir(catch_dir)
            zip_ref.extractall(catch_dir)
    drain_dir = os.path.join(parse_dir, drain_name[:-4])
    drain_dirs.append(drain_dir)
    if not os.path.exists(drain_dir):
        with zipfile.ZipFile(drain, 'r') as zip_ref:
            os.mkdir(drain_dir)
            zip_ref.extractall(drain_dir)
def get_upstream(tree, search_id):
    q = queue.Queue()
    q.put((search_id,))
    upstream = []
    i = 0

    while not q.empty():
        n = q.get()
        if i > 200: #cuts off infinite loops. Number may need to be adjusted if adjoint catchments start to contain more than 200 individual regions
            break
        for s in n:
            upstream.append(s)
            if s in tree:
                q.put(tree[s])
        i += 1
    return upstream

def create_adjoint_dict(catch_dir, drain_dir):
    out_file = os.path.join(out_dir, f'{os.path.basename(catch_dir).split("-")[0]}-upstream-dict.json')
    if not os.path.exists(out_file):
        chmt = gpd.read_file(glob(os.path.join(catch_dir, "*.shp"))[0])
        print(os.path.basename(catch_dir).split("-")[0])
        print(chmt.columns)
        if "NextDownID" in chmt.columns:
            start_time = time.time()
            drain = gpd.read_file(glob(os.path.join(drain_dir, "*.shp"))[0])
            print(drain)
            # drain_pts = gpd.GeoDataFrame(pd.DataFrame(drain).drop('geometry', axis=1),
            #                              geometry=drain.centroid)
            tree = {hydro_id: tuple(chmt[chmt["NextDownID"] == hydro_id]["HydroID"].tolist()) for
                    hydro_id in chmt['HydroID']}
            print(tree)
            upstream_lists_dict = {hydro_id: get_upstream(tree, hydro_id) for hydro_id in chmt["HydroID"]}
            with open(out_file, "w") as f:
                json.dump(upstream_lists_dict, f)
            print("time elapsed: {:.2f}s".format(time.time() - start_time))
        else:
            print("NextDownID not present")
    else:
        print("File already created")
    return out_file


for catch_dir, drain_dir in zip(catch_dirs, drain_dirs):
    create_adjoint_dict(catch_dir, drain_dir)