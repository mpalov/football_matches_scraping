import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score


class MissingDict(dict):
    __missing__ = lambda self, key: key


def make_predictions(data, predictors):
    train = data[data["date"] < '2023-08-01']
    test = data[data["date"] > '2023-08-01']
    rf.fit(train[predictors], train["target"])
    preds = rf.predict(test[predictors])
    combined = pd.DataFrame(dict(actual=test["target"], predicted=preds), index=test.index)
    precision = precision_score(test["target"], preds)
    return combined, precision


def rolling_averages(group, cols, new_cols):
    group = group.sort_values("date")
    rolling_stats = group[cols].rolling(3, closed='left').mean()
    group[new_cols] = rolling_stats
    group = group.dropna(subset=new_cols)
    return group


matches = pd.read_csv("football_data.csv", index_col=0)
del matches["notes"]
del matches["match report"]
del matches["comp"]

matches["date"] = pd.to_datetime(matches["date"])
matches["target"] = (matches["result"] == "W").astype("int")

matches["venue_code"] = matches["venue"].astype("category").cat.codes
matches["opp_code"] = matches["opponent"].astype("category").cat.codes
matches['gf'] = pd.to_numeric(matches['gf'], errors='coerce')
matches['ga'] = pd.to_numeric(matches['ga'], errors='coerce')
matches["hour"] = matches["time"].str.replace(":.+", "", regex=True).astype("int")
matches["day_code"] = matches["date"].dt.dayofweek

rf = RandomForestClassifier(n_estimators=50, min_samples_split=10, random_state=1)
train = matches[matches["date"] < '2023-08-01']
test = matches[matches["date"] > '2023-08-01']
predictors = ["venue_code", "opp_code", "hour", "day_code"]
rf.fit(train[predictors], train["target"])

preds = rf.predict(test[predictors])
acc = accuracy_score(test["target"], preds)

combined = pd.DataFrame(dict(actual=test["target"], predicted=preds))

pd.crosstab(index=combined["actual"], columns=combined["predicted"])

cols = ["gf", "ga", "poss", "sh", "sot", "dist", "fk", "pk", "pkatt"]
new_cols = [f"{c}_rolling" for c in cols]

matches_rolling = matches.groupby("team").apply(lambda x: rolling_averages(x, cols, new_cols))
matches_rolling = matches_rolling.droplevel('team')
matches_rolling.index = range(matches_rolling.shape[0])
combined, precision = make_predictions(matches_rolling, predictors + new_cols)

combined = combined.merge(matches_rolling[["date", "team", "opponent", "result"]], left_index=True, right_index=True)

map_values = {
    "Monchengladbach": " Gladbach",
    "Eintracht Frankfurt": "Eint Frankfurt",
    "Bayer Leverkusen": "Leverkusen",
}
mapping = MissingDict(**map_values)
combined["new_team"] = combined["team"].map(mapping)
merged = combined.merge(combined, left_on=["date", "new_team"], right_on=["date", "opponent"])
print(merged[(merged["predicted_x"] == 1) & (merged["predicted_y"] == 0)]["actual_x"].value_counts()
      )
