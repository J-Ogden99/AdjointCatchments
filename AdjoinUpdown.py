import argparse
import json
import os
import queue
from collections.abc import Iterable
from glob import glob
import geopandas as gpd
import numpy as np
import pandas as pd


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def make_tree(df: pd.DataFrame, order: int = 0) -> dict:
    """
    Makes a dictionary depicting a tree where each segment id as a key has a tuple containing the ids of its parent segments, or the ones that
    have it as the next down id. Either does this for every id in the tree, or only includes ids of a given stream order
    and their parent of the same stream order, if they have one. This function is the original function, it's a lot
    slower but is more stable and well-tested. make_tree_up does the same thing more efficiently, but may need further
    tweaking.
    Args:
        df: dataframe to parse the tree from. Must contain:
            - a column with the segment/catchment ID ("HydroID")
            - a column with the IDs for the next down segment ("NextDownID")
            - an order column
        order: number of stream order to limit it to. If zero, will make tree for all segments,
               otherwise only contains ids that match the given stream order.

    Returns: dictionary where for each key, a tuple of all values that have that key as their next down id is assigned.
             if order==0, values will be either length 0 or 2, otherwise will be 0 or one as only a maximum of
             one parent will be of the given order.
    """
    if order == 0:
        tree = {hydro_id: tuple(df[df["NextDownID"] == hydro_id]["HydroID"].tolist()) for
                hydro_id in df['HydroID']}
        return tree
    tree = {hydro_id: tuple(
        df[df["order_"] == order][df[df["order_"] == order]["NextDownID"] == hydro_id]["HydroID"].tolist()) for
        hydro_id in df["HydroID"]}
    return tree


def make_tree_up(df: pd.DataFrame, order: int = 0, stream_id_col: str = "COMID", next_down_id_col: str = "NextDownID", order_col: str = "order_") -> dict:
    """
    Makes a dictionary depicting a tree where each segment id as a key has a tuple containing the ids of its parent segments, or the ones that
    have it as the next down id. Either does this for every id in the tree, or only includes ids of a given stream order
    and their parent of the same stream order, if they have one. This function attempts to use pandas vectorization and
    indexing to improve efficiency on make_tree, but may miss some edge cases, further testing is underway.
    Args:
        df: dataframe to parse the tree from. Must contain:
            - a column with the segment/catchment ID ("HydroID")
            - a column with the IDs for the next down segment ("NextDownID")
            - an order column
        order: number of stream order to limit it to. If zero, will make tree for all segments,
               otherwise only contains ids that match the given stream order.
        stream_id_col: the name of the column that contains the unique ids for the streams
        next_down_id_col: the name of the column that contains the unique id of the next down stream for each row, the
                          one that the stream for that row feeds into.
        order_col: name of the column that contains the stream order

    Returns: dictionary where for each key, a tuple of all values that have that key as their next down id is assigned.
             if order==0, values will be either length 0 or 2, otherwise will be 0 or one as only a maximum of
             one parent will be of the given order.
    """
    if order == 0:
        out = df[[stream_id_col, next_down_id_col]].set_index(next_down_id_col)
        out.drop(-1, inplace=True)
        tree = {}
        for hydroid in df[stream_id_col]:
            if hydroid in out.index:
                rows = out.loc[hydroid][stream_id_col]
                if not (isinstance(rows, np.floating) or isinstance(rows, np.generic)):
                    tree[hydroid] = tuple(rows.tolist())
                else:
                    tree[hydroid] = (rows,)
            else:
                tree[hydroid] = ()
        return tree
    out = df[df[order_col] == order][[stream_id_col, next_down_id_col, order_col]].set_index(next_down_id_col)
    tree = {hydroid: ((int(out.loc[hydroid][stream_id_col]),) if hydroid in out.index else ()) for hydroid in
            df[df[order_col] == order][stream_id_col]}
    return tree


def make_tree_down(df: pd.DataFrame, order: int = 0, stream_id_col: str = "COMID", next_down_id_col: str = "NextDownID", order_col: str = "order_") -> dict:
    """
    Performs the simpler task of pairing segment ids as keys with their next down ids as values.
    Args:
        df: dataframe to parse the tree from. Must contain:
            - a column with the segment/catchment ID ("HydroID")
            - a column with the IDs for the next down segment ("NextDownID")
            - an order column
        order: number of stream order to limit it to. If zero, will make tree for all segments,
               otherwise only contains ids that match the given stream order.
        stream_id_col: the name of the column that contains the unique ids for the streams
        next_down_id_col: the name of the column that contains the unique id of the next down stream for each row, the
                          one that the stream for that row feeds into.
        order_col: name of the column that contains the stream order

    Returns: dictionary where for each key its next down id from the dataframe is given as a value.
    """
    if order == 0:
        tree = dict(zip(df[stream_id_col], df[next_down_id_col]))
        return tree
    out = df[[stream_id_col, next_down_id_col, order_col]][df[order_col] == order]
    out_2 = out[out[next_down_id_col].isin(out[stream_id_col])]
    out.loc[~out[stream_id_col].isin(out_2[stream_id_col]), next_down_id_col] = -1
    # tree = dict(zip(out.loc[out['NextDownID'] != -1, 'HydroID'], out.loc[out['NextDownID'] != -1, 'NextDownID']))
    tree = dict(zip(out[stream_id_col], out[next_down_id_col]))
    return tree


def trace_tree(tree: dict, search_id: int, cuttoff_n: int = 200) -> list:
    """
    Universal function that traces a tree produced by make_tree_up or make_tree_down from the search id all the way to
    the end of the segment. If the given tree was produced for a given order, it will produce a list with all down or
    upstream segments that share that order. If the tree was produced including all segments, it will get the whole
    upstream network or the whole path to the ocean.
    Args:
        tree: Tree where each key has a tuple containing the ids of each of its parent segments, or an integer of
              a single child, as its value, i.e.:
              {2: (3, 5), 3: (), 4: (): 5: (6, 7), 6: (), 7: ()} for an upstream tree
                or
              {2: -1, 3: 2, 5: 2, 4: -1, 6: 5, 7: 5} for a downstream tree
        search_id: id to search from.

    Returns: list containing all ids that will be upstream of the search_id.
    """
    q = queue.Queue()
    q.put((search_id,))
    upstream = []
    i = 0

    while not q.empty():
        n = q.get()
        if i > cuttoff_n:  # cuts off infinite loops. Number may need to be adjusted if adjoint catchments start to contain more than 200 individual regions
            break
        if isinstance(n, Iterable):
            for s in n:
                if s != -1:
                    upstream.append(s)
                if s in tree:
                    q.put(tree[s])
        else:
            if n != -1:
                upstream.append(n)
            if n in tree:
                q.put(tree[n])
        i += 1
    return upstream


def trace_upstream(tree: dict, search_id: int) -> list:
    """
    Gives a list of all segments that are upstream of a given id when given a tree that contains the parent ids of each
    key. This is intended for use with trees produced for a certain order, where each key has only one or no values.
    Args:
        tree: Tree produced using make_tree_up for a given order, not for all ids.
        search_id: id to search from.

    Returns: list containing all ids in the chain of same_order streams upstream of the search_id.
    """

    upstream = [int(search_id)]
    up_id = tree[search_id]
    while len(up_id) != 0:
        upstream.append(int(up_id[0]))
        up_id = tree[up_id[0]]
    return upstream


def trace_downstream(tree: dict, search_id: int) -> list:
    """
    Gives a list of all segments that are upstream of a given id when given a tree that contains the parent ids of each
    key. This is intended for use with trees produced for a certain order, where each key has only one or no values.
    Args:
        tree: Tree produced using make_tree_up for a given order, not for all ids.
        search_id: id to search from.

    Returns: list containing all ids in the chain of same_order streams upstream of the search_id.

    """
    downstream = [int(search_id)]
    down_id = tree[search_id]
    while down_id in tree:
        downstream.append(int(down_id))
        down_id = tree[down_id]
    return downstream

def join_order_geoglows(catch, drain):
    drain_pts = gpd.GeoDataFrame(pd.DataFrame(drain).drop('geometry', axis=1),
                                         geometry=drain.centroid)
    comb = catch.sjoin(drain_pts[['COMID', 'order_', 'geometry']], how='left', predicate='contains')
    comb.reset_index(inplace=True, drop=True)
    comb.drop('index_right', axis=1, inplace=True)
    comb_adjoin = comb.copy()
    comb_adjoin = comb_adjoin.dissolve(by='HydroID', as_index=False, aggfunc='max')
    comb_adjoin = comb_adjoin.sort_values(by='HydroID', ascending=True)
    return comb_adjoin


def create_adjoint_dict(network_dir, out_file: str = None, stream_id_col: str = "COMID",
                        next_down_id_col: str = "NextDownID", order_col: str = "order_", trace_up: bool = True,
                        order_filter: int = 0):
    """
    Creates a dictionary where each unique id in a stream network is assigned a list of all ids upstream or downstream
    of that stream, as specified. By default is designed to trace upstream on GEOGloWS Delineation Catchment shapefiles,
    but can be customized for other files with column name parameters, customized to trace down, or filtered by stream
    order. If filtered by stream order, the dictionary will only contain ids of the given stream order, with the
    upstream or downstream ids for the other streams in the chain that share that stream order.
    Args:
        network_dir: path to directory that contains .shp file and all necessary indexing files (.shx, etc.). This file
                     must contain attributes for a unique id and a next down id, and if filtering by order number is
                     specified, it must also contain a column with stream order values.
        out_file: a path to an output file to write the dictionary as a .json, if desired.
        stream_id_col: the name of the column that contains the unique ids for the stream segments
        next_down_id_col: the name of the column that contains the unique id of the next down stream for each row, the
                          one that the stream for that row feeds into.
        order_col: name of the column that contains the stream order
        trace_up: if true, trace up from each stream, otherwise trace down.
        order_filter: if set to number other than zero, limits values traced to only ids that match streams with that
                      stream order

    Returns:

    """
    network_df = gpd.read_file(glob(os.path.join(network_dir, "*.shp"))[0])
    columns_to_search = [stream_id_col, next_down_id_col]
    if order_filter != 0:
        columns_to_search.append(order_col)
    for col in columns_to_search:
        if col not in network_df.columns:
            print(f"Column {col} not present")
            return {}
    if trace_up:
        tree = make_tree_up(network_df, order_filter, stream_id_col, next_down_id_col, order_col)
    else:
        tree = make_tree_down(network_df, order_filter, stream_id_col, next_down_id_col, order_col)
    upstream_lists_dict = {str(hydro_id): trace_tree(tree, hydro_id) for hydro_id in network_df[stream_id_col]}
    if out_file is not None:
        if not os.path.exists(out_file):
            with open(out_file, "w") as f:
                json.dump(upstream_lists_dict, f, cls=NpEncoder)
        else:
            print("File already created")
            return {}
    return upstream_lists_dict


if __name__ == "__main__":
    # catch = gpd.read_file(glob(os.path.join(sys.argv[1], "*.shp")))
    # Default values
    network_dir = None
    out_file = None
    stream_id_col = "HydroID"
    next_down_id_col = "NextDownID"
    order_col = "order_"
    trace_up = True
    order_filter = 0
    description = 'This script will run Adjoin Catchments code on a shapefile. The function produces a dictionary,' \
                  ' which can be written to a specified destination as a .json file if the "outfile" parameter is ' \
                  'defined. The function opens the shapefile provided, and for each stream id finds a list of all ' \
                  'stream ids that are upstream, or downstream, as specified, either filtering by stream order or ' \
                  'getting all stream segments. A dictionary is created with each id as the key, and the list of their'\
                  ' up or downstream parents/children as their value.'

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('networkdir', type=str,
                        help='Required. Path to directory containing .shp file. This directory must only contain the '
                             'target shapefile')
    parser.add_argument('--outfile', metavar='-O', type=str,
                        help='Path to output file if writing to .json is desired. Default: None')
    parser.add_argument('--streamidcol', metavar='-SIDCol', type=str, default="COMID",
                        help='Name of Stream ID Column. Default: "COMID"')
    parser.add_argument('--nextdownidcol', metavar='-NDIDCol', type=str, default="NextDownID",
                        help='Name of Next Down ID Column. Default: "NextDownID"')
    parser.add_argument('--ordercol', metavar='-OrdCol', type=str, default="order_",
                        help='Name of Column containing stream orders. Need not be provided if orderfilter is 0,'
                             'otherwise required if the tool is to be able to filter by order. Default: "order_"')
    parser.add_argument('--traceup', metavar='-U', type=bool, default=True,
                        help='If true, traces up, else down. Accepts: True or False. Default: True')
    parser.add_argument('--orderfilter', metavar='-Ord', type=int, default=0,
                        help='Number of stream order to limit to. If 0 runs on all streams, else only includes '
                             'specified stream order. Default: 0.')
    args = parser.parse_args()
    print(vars(args))
    network_dir = args.networkdir
    if 'outfile' in args:
        out_file = args.outfile
    if 'streamidcol' in args:
        stream_id_col = args.streamidcol
    if 'nextdownidcol' in args:
        next_down_id_col = args.nextdownidcol
    if 'ordercol' in args:
        order_col = args.ordercol
    if 'traceup' in args:
        trace_up = args.traceup
    if 'orderfilter' in args:
        order_filter = args.orderfilter

    print(create_adjoint_dict(network_dir, out_file, stream_id_col, next_down_id_col, order_col, trace_up, order_filter))
    # catch = gpd.read_file(glob(os.path.join("scratch_data/japan_comb_sorted", "*.shp"))[0])
    # out_file = os.path.join(sys.argv[1], "tree_3.json") #path to directory in which jsons must be written should be given as argument when running script
    # for i in range(5):
    #     print(i)
    #     tree = make_tree_up(catch, i)
    #     out_file = os.path.join("scratch_data/trees_by_order", f"up_tree_order{i}.json")
    #     with open(out_file, "w") as f:
    #         json.dump(tree, f)
    # for i in range(5):
    #     print(i)
    #     tree = make_tree_up(catch, i)
    #     out_file = os.path.join("scratch_data/trees_by_order", f"down_tree_order{i}.json")
    #     with open(out_file, "w") as f:
    #         json.dump(tree, f)


