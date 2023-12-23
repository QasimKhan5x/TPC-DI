import os

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# Database connection details
host = "localhost"
user = "root"
password = "password"
database = "tpcdi_sf5"

# Create the SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}/{database}?allow_local_infile=true"
)

# ## Audit

BATCH_DIRS = [
    "data\\sf5\\Batch1\\",
    "data\\sf5\\Batch2\\",
    "data\\sf5\\Batch3\\",
]
BATCH_FILES = [
    "data\\sf5\\Batch1_audit.csv",
    "data\\sf5\\Batch2_audit.csv",
    "data\\sf5\\Batch3_audit.csv",
]
audit_dtypes = {
    "DataSet": str,
    "BatchID": int,
    "Date": "datetime64",
    "Attribute": str,
    "Value": int,
    "DValue": float,
}


df = pd.read_csv(BATCH_FILES[0], dtype=audit_dtypes)
df = pd.concat([df, pd.read_csv(BATCH_FILES[1], dtype=audit_dtypes)])
df = pd.concat([df, pd.read_csv(BATCH_FILES[2], dtype=audit_dtypes)])
df = pd.concat([df, pd.read_csv(r"data\sf5\Generator_audit.csv", dtype=audit_dtypes)])

for folder in BATCH_DIRS:
    files = os.listdir(folder)
    files = list(filter(lambda x: "_audit" in x, files))
    for file in files:
        df = pd.concat([df, pd.read_csv(folder + file, dtype=audit_dtypes)])

df.columns = df.columns.str.strip()

df.to_sql(
    "audit",
    engine,
    if_exists="append",
    index=False,
    dtype={
        "DataSet": sqlalchemy.types.CHAR(20),
        "BatchID": sqlalchemy.types.SmallInteger,
        "Date": sqlalchemy.types.Date,
        "Attribute": sqlalchemy.types.CHAR(50),
        "Value": sqlalchemy.types.BigInteger,
        "DValue": sqlalchemy.types.Numeric(precision=15, scale=5),
    },
)
