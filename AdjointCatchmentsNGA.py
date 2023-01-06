import json
import os
import queue
from glob import glob
import geopandas as gpd
import time
import contextily as cx
import matplotlib.pyplot as plt
start_time = time.time()

parse_dir = 'NGADelineation'
out_dir = 'NGADelineation/OutputJSONs'
rivid_field = "streamID"
next_down_field = 'DSLINKNO'
catchments = glob(os.path.join(parse_dir, '*/*basins*.gpkg'))
drainagelines = glob(os.path.join(parse_dir, '*/*streamnet*.shp'))

catch = gpd.read_file(catchments[0])
drain = gpd.read_file((drainagelines[0]))
print(drain.columns)
catch_stream = catch.merge(drain.drop('geometry', axis=1), left_on='streamID', right_on='LINKNO').drop('LINKNO', axis=1)
catch_stream.to_file(os.path.join(parse_dir, 'Japan_comb/Japan_comb.shp'))
plot_group = catch_stream
print(plot_group)
ax = plot_group.plot(alpha=0.5)
drain = drain[drain['LINKNO'].isin(plot_group['streamID'])]
print(drain)
drain.plot(ax=ax)
try:
    cx.add_basemap(ax, zoom=12)
except:
    print("Error in getting basemap, area likely too small to get tiles of sufficient resolution")

def get_upstream(tree, search_id):
    q = queue.Queue()
    q.put((search_id,)) #how to make this a tuple?
    upstream = []
    i = 0

    while not q.empty():
        n = q.get()
        if i > 200:
            break
        for s in n:
            upstream.append(s)
            if s in tree:
                q.put(tree[s])
        i += 1
    return upstream


tree = {rivid: tuple(catch_stream[catch_stream[next_down_field] == rivid][rivid_field].tolist()) for
        rivid in catch_stream[rivid_field]}

upstream_lists_dict = {rivid: get_upstream(tree, rivid) for rivid in catch_stream[rivid_field]}
out_file = os.path.join(out_dir, f'Japan-nga-upstream-dict.json')
with open(out_file, "w") as f:
    json.dump(upstream_lists_dict, f)

print ("time elapsed: {:.2f}s".format(time.time() - start_time))