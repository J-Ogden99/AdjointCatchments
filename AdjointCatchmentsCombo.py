import geopandas as gpd
import pandas as pd
import numpy as np
import os
import queue
import json
import io
from shapely.ops import unary_union
import matplotlib.pyplot as plt

pd.options.mode.chained_assignment = None

japan_dir = os.path.join('/Users/joshogden/PycharmProjects/AdjointCatchments/GEOGloWS-Delineation-Shapefiles')
if not os.path.exists('scratch_data/japan_comb_sorted/japan_comb_sorted.shp'):
    japan_chmt = gpd.read_file(os.path.join(japan_dir,'japan-geoglows-catchment/japan-geoglows-catchment.shp'))
    japan_drain = gpd.read_file(os.path.join(japan_dir, 'japan-geoglows-drainageline/japan-geoglows-drainageline.shp'))
    japan_drain_pts = gpd.GeoDataFrame(pd.DataFrame(japan_drain).drop('geometry', axis=1), geometry=japan_drain.centroid)
    # print(japan_drain[['order_','geometry']])
    japan_comb = japan_chmt.sjoin(japan_drain_pts[['COMID', 'order_','geometry']], how='left', predicate='contains')
    # print(japan_comb.columns, japan_comb.order_.head())
    japan_comb.reset_index(inplace=True, drop=True)
    japan_comb.drop('index_right', axis=1, inplace=True)
    japan_comb_adjoin = japan_comb.copy()
    japan_comb_adjoin = japan_comb_adjoin.dissolve(by='HydroID', as_index=False, aggfunc='max')
    japan_comb_adjoin.sort_values(by='HydroID', ascending=True).to_csv('scratch_data/japan_comb_sorted.csv')
    japan_comb_adjoin.sort_values(by='HydroID', ascending=True).to_file('scratch_data/japan_comb_sorted/japan_comb_sorted.shp')
    japan_chmt.sort_values(by='HydroID', ascending=True).to_csv('scratch_data/japan_chmt_sorted.csv')
else:
    japan_comb_adjoin = gpd.read_file('scratch_data/japan_comb_sorted/japan_comb_sorted.shp')
nans = list(japan_comb_adjoin[japan_comb_adjoin['order_'].isna()]['HydroID'])
with open('scratch_data/NanOrders.txt', 'w') as out:
    for val in nans:
        out.write(f'{val}\n')
# japan_comb_adjoin = japan_comb_adjoin.dropna(subset=['order_'])
hydroids = japan_comb_adjoin['HydroID'].to_numpy()
nextdownids = japan_comb_adjoin['NextDownID'].to_numpy()
orders = japan_comb_adjoin['order_'].to_numpy()
adjoint_ids = []

def adjoin_downstream(hydroid, nextdownid, order, adjoint_ids: list):
    adjoint_ids.append(hydroid)
    if nextdownid == -1:
        return adjoint_ids
    if len(japan_comb_adjoin[japan_comb_adjoin['HydroID'] == nextdownid]) == 0:
        adjoint_ids.append(nextdownid)
        return adjoint_ids
    if int(japan_comb_adjoin[japan_comb_adjoin['HydroID'] == nextdownid]['order_']) > order:
        adjoint_ids.append(nextdownid)
        return adjoint_ids
    return adjoin_downstream(nextdownid, int(japan_comb_adjoin[japan_comb_adjoin['HydroID'] == nextdownid]['NextDownID']), order, adjoint_ids)
def get_upstream(tree, search_id):
    q = queue.Queue()
    q.put((search_id,)) #how to make this a tuple?
    upstream = []

    while not q.empty():
        n = q.get()
        print(n)
        for s in n:
            upstream.append(s)
            if s in tree:
                q.put(tree[s])
    return upstream

def join_all(df):
    adjoint_catchments = []
    hydroids = df['HydroID'].to_numpy()
    nextdownids = df['NextDownID'].to_numpy()
    orders = df['order_'].to_numpy()
    for id, nextdown_id, order in zip(hydroids, nextdownids, orders):
        adjoint_id_list = []
        adjoint_catchments.append(adjoin_downstream(id, nextdown_id, order, adjoint_id_list))
    adjoint_catchments = np.array(adjoint_catchments, dtype=object)
    print(adjoint_catchments)
    #Get upstream for each segment
    upstream_dict = {}
    last_elem = np.array([lst[-1] for lst in adjoint_catchments])
    for id in hydroids:
        indices = np.where(last_elem == id)[0]
        chains_that_end_with_id = np.take(adjoint_catchments, indices, 0)
        upstream_set = set()
        for chain in chains_that_end_with_id:
            upstream_set |= set(chain)
        upstream_dict[str(id)] = id if len(upstream_set) == 0 else list(upstream_set)
    print(upstream_dict)
    for id, chain in upstream_dict.items():
        new_chain = set(chain)
        for upstream_id in chain:
            if upstream_id != id:
                while True:
                    new_chain |= set(upstream_dict[upstream_id])
                    #todo need something recursive to keep following the chain up
                    if len(upstream_dict[upstream_id]) > 1:
                        break

    out = []
    #Get whole catchments
    while len(adjoint_catchments) > 0:
        first, *rest = adjoint_catchments
        first = set(first)

        lf = -1
        while len(first) > lf:
            lf = len(first)

            rest2 = []
            for r in rest:
                if len(first.intersection(set(r))) > 0:
                    first |= set(r)
                else:
                    rest2.append(r)
            rest = rest2

        out.append(first)
        adjoint_catchments = rest
    out = [list(st) for st in out]
    return out

# adj_chmt = join_all(japan_comb_adjoin)

tree = {hydroid: tuple(japan_comb_adjoin[japan_comb_adjoin["NextDownID"] == hydroid]["HydroID"].tolist()) for hydroid in japan_comb_adjoin['HydroID']}
print(get_upstream(tree, 100))

upstream_lists_dict = {hydroid: get_upstream(tree, hydroid) for hydroid in japan_comb_adjoin["HydroID"]}
with open("upstream_segments.json", "w") as f:
    json.dump(upstream_lists_dict, f)
# adjoint_catchments_df = gpd.GeoDataFrame(columns = pd.DataFrame(japan_comb_adjoin).columns)
# for i, id in enumerate(upstream_lists_dict.keys()):
#     adjoint_catchments_df = pd.concat([adjoint_catchments_df, japan_comb_adjoin[japan_comb_adjoin["HydroID"].isin(upstream_lists_dict[id])].dissolve()])
#     adjoint_catchments_df.iloc[i]['HydroID'] = id
# gpd.GeoDataFrame(adjoint_catchments_df).to_file('scratch_data/adjoint_catchments/adjoint_catchments.shp')
# print(adjoint_catchments_df)

# upstream_lists_dict = {hydroid: set(get_upstream(hydroid, japan_comb_adjoin, [])) for hydroid in japan_comb_adjoin['HydroID']}
#
print(upstream_lists_dict)