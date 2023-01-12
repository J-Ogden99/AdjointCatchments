import json
import pandas as pd
import argparse
import geopandas as gpd
import contextily as cx
import matplotlib.pyplot as plt

if __name__ == "__main__":
    network_shp = None
    upstream_json_path = None
    stream_id_col = "HydroID"
    order_col = "order_"
    description = 'This script will run some tests on JSONs produced from AdjoinUpdown.py. Given a path to a stream' \
                  'network and a path to an upstreamJSON produced using that same stream network, it will display a' \
                  'map showing the full stream network with the upstream chain from a chosen ID highlighted, and it' \
                  'will also test the list going back downstream from the top to the bottom to ensure it is right.'

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('networkshp', type=str,
                        help='Required. Path to .shp file containing network to test.')
    parser.add_argument('upstreamjsonpath', type=str,
                        help='Required. Path to .shp file containing json created from the network using '
                             'AdjoinUpdown.py')
    parser.add_argument('--streamidcol', metavar='-SIDCol', type=str, default="COMID",
                        help='Name of Stream ID Column. Default: "COMID"')
    parser.add_argument('--ordercol', metavar='-OrdCol', type=str, default="order_",
                        help='Name of Column containing stream orders. Need not be provided if orderfilter is 0,'
                             'otherwise required if the tool is to be able to filter by order. Default: "order_"')
    args = parser.parse_args()
    print(vars(args))
    network_shp = args.networkshp
    upstream_json_path = args.upstreamjsonpath
    if 'streamidcol' in args:
        stream_id_col = args.streamidcol
    if 'ordercol' in args:
        order_col = args.ordercol

    f = open(upstream_json_path)

    upstream_dict = json.load(f)

    # japan_comb_adjoin = gpd.read_file('NGADelineation/Japan_comb/Japan_comb.shp')
    # print(japan_comb_adjoin['streamID'])
    drainage = gpd.read_file(network_shp)
    searchid = input('input search id: ')
    while searchid != "stop":
        fig, ax = plt.subplots(figsize=(100, 100))
        print(upstream_dict[searchid])
        # print(japan_comb_adjoin[japan_comb_adjoin["streamID"] == searchid])
        id_list = upstream_dict[searchid]
        plot_group = drainage[drainage[stream_id_col].isin(id_list)]
        print(plot_group)
        plot_group.plot(ax=ax, color='red')
        # drain = japan_drainage[japan_drainage['LINKNO'].isin(plot_group['streamID'])]
        drainage.plot(ax=ax, alpha=0.5, color='blue')
        try:
            cx.add_basemap(ax)
        except:
            print("Error in getting basemap, area likely too small to get tiles of sufficient resolution")
        plt.show()
        searchid = input('input search id: ')