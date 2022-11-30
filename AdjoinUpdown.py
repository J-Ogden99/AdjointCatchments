import json
import os
import sys
import queue
import time
from glob import glob
from collections.abc import Iterable
import geopandas as gpd
import numpy as np
import pandas as pd


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


def make_tree_up(df: pd.DataFrame, order: int = 0) -> dict:
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

    Returns: dictionary where for each key, a tuple of all values that have that key as their next down id is assigned.
             if order==0, values will be either length 0 or 2, otherwise will be 0 or one as only a maximum of
             one parent will be of the given order.
    """
    if order == 0:
        out = df[["HydroID", "NextDownID", "order_"]].set_index("NextDownID")
        out.drop(-1, inplace=True)
        tree = {}
        for hydroid in df["HydroID"]:
            if hydroid in out.index:
                rows = out.loc[hydroid]["HydroID"]
                if not isinstance(rows, np.floating):
                    tree[hydroid] = tuple(rows.tolist())
                else:
                    tree[hydroid] = (rows,)
            else:
                tree[hydroid] = ()
        return tree
    out = df[df["order_"] == order][["HydroID", "NextDownID", "order_"]].set_index("NextDownID")
    tree = {hydroid: ((int(out.loc[hydroid]["HydroID"]),) if hydroid in out.index else ()) for hydroid in
            df[df['order_'] == order]["HydroID"]}
    return tree


def make_tree_down(df: pd.DataFrame, order: int = 0) -> dict:
    """
    Performs the simpler task of pairing segment ids as keys with their next down ids as values.
    Args:
        df: dataframe to parse the tree from. Must contain:
            - a column with the segment/catchment ID ("HydroID")
            - a column with the IDs for the next down segment ("NextDownID")
            - an order column
        order: number of stream order to limit it to. If zero, will make tree for all segments,
               otherwise only contains ids that match the given stream order.

    Returns: dictionary where for each key its next down id from the dataframe is given as a value.
    """
    if order == 0:
        tree = dict(zip(df['HydroID'], df['NextDownID']))
        return tree
    out = chmt[["HydroID", "NextDownID", "order_"]][chmt["order_"] == order]
    out_2 = out[out["NextDownID"].isin(out["HydroID"])]
    out.loc[~out["HydroID"].isin(out_2["HydroID"]), "NextDownID"] = -1
    # tree = dict(zip(out.loc[out['NextDownID'] != -1, 'HydroID'], out.loc[out['NextDownID'] != -1, 'NextDownID']))
    tree = dict(zip(out['HydroID'], out['NextDownID']))
    return tree


def trace_tree(tree: dict, search_id: int) -> list:
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
        if i > 200:  # cuts off infinite loops. Number may need to be adjusted if adjoint catchments start to contain more than 200 individual regions
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


if __name__ == "__main__":
    chmt = gpd.read_file(glob(os.path.join("scratch_data/japan_comb_sorted", "*.shp"))[0])
    # out_file = os.path.join(sys.argv[1], "tree_3.json") #path to directory in which jsons must be written should be given as argument when running script
    # for i in range(5):
    #     print(i)
    #     tree = make_tree_up(chmt, i)
    #     out_file = os.path.join("scratch_data/trees_by_order", f"up_tree_order{i}.json")
    #     with open(out_file, "w") as f:
    #         json.dump(tree, f)
    # for i in range(5):
    #     print(i)
    #     tree = make_tree_up(chmt, i)
    #     out_file = os.path.join("scratch_data/trees_by_order", f"down_tree_order{i}.json")
    #     with open(out_file, "w") as f:
    #         json.dump(tree, f)

    # Example of how to use the functions to trace up and down from each node for a given order and store in a dictionary (all upstream or downstream segments of the same order)
    tree_3 = make_tree_up(chmt, 4)
    # print({hydroid: get_upstream(tree_3, hydroid) for hydroid in tree_3.keys()})
    tree_3_down = make_tree_down(chmt, 4)
    print({hydroid: trace_tree(tree_3_down, hydroid) for hydroid in tree_3_down.keys()})
    upstream_1 = {hydroid: trace_tree(tree_3, hydroid) for hydroid in tree_3.keys()}
    # print(upstream_1)
    downstream_1 = {hydroid: trace_tree(tree_3_down, hydroid) for hydroid in tree_3_down.keys()}
    print(downstream_1)
