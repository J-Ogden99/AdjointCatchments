import json
import pandas as pd
import geopandas as gpd
import contextily as cx
import matplotlib.pyplot as plt

f = open('/Users/joshogden/PycharmProjects/AdjointCatchments/NGA Delineation/OutputJSONs/Japan-nga-upstream-dict.json')

upstream_dict = json.load(f)

japan_comb_adjoin = gpd.read_file('NGA Delineation/Japan_comb/Japan_comb.shp')
print(japan_comb_adjoin['streamID'])
japan_drainage = gpd.read_file('NGA Delineation/Japan/TDX_streamnet_4020034510_01.shp')
searchid = input('input search id: ')
while searchid != "stop":
    print(upstream_dict[searchid])
    print(japan_comb_adjoin[japan_comb_adjoin["streamID"] == searchid])
    plot_group = japan_comb_adjoin[japan_comb_adjoin["streamID"].isin(upstream_dict[searchid])]
    print(plot_group)
    ax = plot_group.plot(alpha=0.5)
    drain = japan_drainage[japan_drainage['LINKNO'].isin(plot_group['streamID'])]
    drain.plot(ax=ax)
    try:
        cx.add_basemap(ax)
    except:
        print("Error in getting basemap, area likely too small to get tiles of sufficient resolution")
    plt.show()
    searchid = input('input search id: ')