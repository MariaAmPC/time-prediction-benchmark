import glob
import pandas as pd
import os
import numpy as np
import pickle
from sys import argv
import json

# cv_results_dir = argv[1] # cv_results
# outfile = argv[2] #"training_params.pkl"

cv_results_dir = r"C:\Users\49170\Documents\FAU\Diehl Seminar\results\cv_results_rf_single_agg_diehl.csv"
outfile = "training_params"

data = pd.read_csv(cv_results_dir, sep=";")

# select best params according to auc only
data = data[data.metric == "mae"]

# fix cases where score is unknown
data.loc[pd.isnull(data["score"]), "score"] = 0

if data["score"].dtype != np.float64:
    data["score"][data["score"] == "None"] = 0

data["score"] = data["score"].astype(float)
data.fillna(0, inplace=True)

# extract columns that refer to parameters
params_cols = [col for col in data.columns if
               col not in ['cls', 'dataset', 'method', 'metric', 'nr_events', 'part', 'score']]

# aggregate data over all CV folds
data_agg = data.groupby(["cls", "dataset", "method", "metric", "nr_events"] + params_cols, as_index=False)[
    "score"].mean()
data_agg_over_all_prefixes = data.groupby(["cls", "dataset", "method", "metric"] + params_cols, as_index=False)[
    "score"].mean()

# select the best params - lowest MAE
data_best = data_agg.sort_values("score", ascending=True).groupby(["cls", "dataset", "method", "metric", "nr_events"],
                                                                  as_index=False).first()
data_best_over_all_prefixes = data_agg_over_all_prefixes.sort_values("score", ascending=True).groupby(
    ["cls", "dataset", "method", "metric"], as_index=False).first()

best_params = {}

# all except prefix length based
for row in data_best_over_all_prefixes[~data_best_over_all_prefixes.method.str.contains("prefix")][
            ["dataset", "method", "cls"] + params_cols].values:

    if row[0] not in best_params:
        best_params[row[0]] = {}
    if row[1] not in best_params[row[0]]:
        best_params[row[0]][row[1]] = {}
    if row[2] not in best_params[row[0]][row[1]]:
        best_params[row[0]][row[1]][row[2]] = {}

    for i, param in enumerate(params_cols):
        value = row[3 + i]
        if param == "max_features":
            value = value if value == "sqrt" else float(value)
        elif param in ["n_clusters", "n_estimators", "n_neighbors", "max_depth"]:
            value = int(value)
        elif param == "learning_rate":
            value = float(value)

        best_params[row[0]][row[1]][row[2]][param] = value

# only prefix length based
for row in data_best[data_best.method.str.contains("prefix")][
            ["dataset", "method", "cls", "nr_events"] + params_cols].values:

    if row[0] not in best_params:
        best_params[row[0]] = {}
    if row[1] not in best_params[row[0]]:
        best_params[row[0]][row[1]] = {}
    if row[2] not in best_params[row[0]][row[1]]:
        best_params[row[0]][row[1]][row[2]] = {}
    if row[3] not in best_params[row[0]][row[1]][row[2]]:
        best_params[row[0]][row[1]][row[2]][row[3]] = {}

    for i, param in enumerate(params_cols):
        value = row[4 + i]
        if param == "max_features":
            value = value if value == "sqrt" else float(value)
        elif param in ["n_clusters", "n_estimators", "n_neighbors", "max_depth"]:
            value = int(value)
        elif param == "learning_rate":
            value = float(value)

        best_params[row[0]][row[1]][row[2]][row[3]][param] = value

# write to file
with open(r"C:\Users\49170\Documents\FAU\Diehl Seminar\results\%s.pkl" % outfile, "wb") as fout:
    pickle.dump(best_params, fout)

with open(r"C:\Users\49170\Documents\FAU\Diehl Seminar\results\%s.json" % outfile, "w") as fout:
    json.dump(best_params, fout, indent=3)
