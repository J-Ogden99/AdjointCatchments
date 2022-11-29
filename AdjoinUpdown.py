import json
import os
import queue
import time
from glob import glob
import geopandas as gpd
import numpy as np
import pandas as pd


def make_tree(df: pd.DataFrame, order: int = 0):
    if order == 0:
        tree = {hydro_id: tuple(df[df["NextDownID"] == hydro_id]["HydroID"].tolist()) for
                hydro_id in df['HydroID']}
        return tree
    tree = {hydro_id: tuple(
        df[df["order_"] == order][df[df["order_"] == order]["NextDownID"] == hydro_id]["HydroID"].tolist()) for
        hydro_id in df["HydroID"]}
    return tree


def get_upstream(tree, search_id):
    q = queue.Queue()
    q.put((search_id,))
    upstream = []
    i = 0

    while not q.empty():
        n = q.get()
        if i > 200:  # cuts off infinite loops. Number may need to be adjusted if adjoint catchments start to contain more than 200 individual regions
            break
        for s in n:
            upstream.append(s)
            if s in tree:
                q.put(tree[s])
        i += 1
    return upstream


def trace_upstream(tree, search_id):
    upstream = [int(search_id)]
    up_id = tree[str(search_id)]
    while len(up_id) != 0:
        upstream.append(int(up_id[0]))
        up_id = tree[str(up_id[0])]
    return upstream


def trace_downstream(tree, search_id):
    downstream = [int(search_id)]
    down_id = tree[search_id]
    print(down_id)
    while down_id != -1:
        downstream.append(int(down_id))
        down_id = tree[down_id]
    return downstream


def make_tree_up(df: pd.DataFrame, order: int = 0):
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


def make_tree_down(df: pd.DataFrame, order: int = 0):
    if order == 0:
        tree = dict(zip(df['HydroID'], df['NextDownID']))
        return tree
    out = chmt[["HydroID", "NextDownID", "order_"]][chmt["order_"] == order]
    out_2 = out[out["NextDownID"].isin(out["HydroID"])]
    out.loc[~out["HydroID"].isin(out_2["HydroID"]), "NextDownID"] = -1
    tree = dict(zip(out['HydroID'], out['NextDownID']))
    return tree


if __name__ == "__main__":
    chmt = gpd.read_file(glob(os.path.join("scratch_data/japan_comb_sorted", "*.shp"))[0])
    out_file = os.path.join("scratch_data", "tree_3.json")
    for i in range(5):
        print(i)
        tree = make_tree_up(chmt, i)
        out_file = os.path.join("scratch_data/trees_by_order", f"up_tree_order{i}.json")
        with open(out_file, "w") as f:
            json.dump(tree, f)
    for i in range(5):
        print(i)
        tree = make_tree_up(chmt, i)
        out_file = os.path.join("scratch_data/trees_by_order", f"down_tree_order{i}.json")
        with open(out_file, "w") as f:
            json.dump(tree, f)
    # Example of how to use the functions to trace up and down from each node for a given order and store in a dictionary (all upstream or downstream segments of the same order)
    # tree_3 = make_tree_up(chmt, 3)
    # tree_3_down = make_tree_down(chmt, 3)
    # upstream_1 = {hydroid: trace_upstream(tree_3, hydroid) for hydroid in tree_3.keys()}
    # downtream_1 = {hydroid: trace_downstream(tree_3_down, hydroid) for hydroid in tree_3_down.keys()}
