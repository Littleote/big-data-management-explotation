import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
import joblib
import os

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

os.chdir(Path(os.getcwd()).parent)
EXPLOITATION_PREDICTIVE_DB = "exploitation/data_predictive.db"
EXPLOITATION_MODEL_DB = "exploitation/model_store.db"

print(f"Loading data from {EXPLOITATION_PREDICTIVE_DB}.")
con = duckdb.connect(database=EXPLOITATION_PREDICTIVE_DB)
data = con.execute("SELECT * FROM sales").fetchdf()
con.close()


# Preprocess (remove NA and minor changes)
print("Preprocessing...")
data["floor"] = data["floor"].replace({"bj": 0})
data["floor"] = pd.to_numeric(data["floor"], errors="coerce")

features = [
    "bathrooms",
    "typology",
    "distance",
    "district",
    "exterior",
    "size",
    "rooms",
    "floor",
    "hasLift",
    "neighborhood_id",
    "population",
    "RFD",
    "move_in",
    "move_out",
]
target = "price"

data = data.dropna(subset=[target])

# Train/Test split

X = data[features]
y = data[target]

X = X.dropna(subset=features)
y = y[X.index]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# Imputation for categorical and numerical columns

categorical_features = [
    "typology",
    "district",
    "exterior",
    "hasLift",
    "neighborhood_id",
]
numeric_features = [
    "bathrooms",
    "distance",
    "size",
    "rooms",
    "floor",
    "population",
    "RFD",
    "move_in",
    "move_out",
]

numeric_transformer = Pipeline(
    steps=[("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]
)

categorical_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ]
)

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features),
    ]
)

# Model it with a regresion RandomForest

print(f"Training the model with {len(data)} entries.")
model = Pipeline(
    steps=[
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(n_estimators=100, random_state=42)),
    ]
)

model.fit(X_train, y_train)


# Predictions and evaluation

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = root_mean_squared_error(y_test, y_pred)

print("Mean Absolute Error (MAE):", mae)
print("Root Mean Squared Error (RMSE):", rmse)

# Storing the model

model_filename = "model.joblib"
joblib.dump(model, model_filename)

with open(model_filename, "rb") as file:
    model_binary = file.read()

model_data = {
    "model": [model_binary],
    "timestamp": [datetime.now()],
    "mae": [mae],
    "rmse": [rmse],
    "entries": [len(data)],
}
model_df = pd.DataFrame(model_data)

print(f"Saving the model in {EXPLOITATION_MODEL_DB}.")
con = duckdb.connect(database=EXPLOITATION_MODEL_DB)

# Create db if it doesn't exist
con.execute("""
CREATE TABLE IF NOT EXISTS model_store (
    model BLOB,
    timestamp TIMESTAMP,
    mae DOUBLE,
    rmse DOUBLE,
    entries INTEGER
)
""")

con.execute("INSERT INTO model_store SELECT * FROM model_df")

df = con.execute("SELECT * FROM model_store").fetchdf()
print("Data from table: model_store")
print(df.head(), "\n")
con.close()

os.remove(model_filename)
