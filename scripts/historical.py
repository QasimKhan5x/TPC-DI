import os
from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy
from lxml import etree
from sqlalchemy import create_engine, text
from tqdm import tqdm, trange

from scripts.util import execute_batch_validation

import warnings
warnings.filterwarnings('ignore')

# Database connection details
host = "localhost"
user = "root"
password = "password"
sf = 5
database = f"tpcdi_sf{sf}"

# Create the SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}/{database}?allow_local_infile=true"
)

with open("scripts\\preparation.sql") as f:
    queries = f.read().split(";")
with engine.connect() as cxn:
    for query in queries:
        if len(query.strip()) > 0:
            cxn.execute(text(query + ";"))

execute_batch_validation(engine)

# ## Historical Load
start_time = datetime.now()
print("Historical Load started at", start_time)


BATCH_ID = 1
DATA_DIR = f"data\\sf{sf}\\Batch{BATCH_ID}\\"


with open(DATA_DIR + "BatchDate.txt", "r") as f:
    BATCH_DATE = f.read().strip()
BATCH_DATE = pd.to_datetime(BATCH_DATE, format="%Y-%m-%d")


# ### dimDate


date_df = pd.read_csv(
    DATA_DIR + "Date.txt",
    sep="|",
    header=None,
    names=[
        "SK_DateID",
        "DateValue",
        "DateDesc",
        "CalendarYearID",
        "CalendarYearDesc",
        "CalendarQtrID",
        "CalendarQtrDesc",
        "CalendarMonthID",
        "CalendarMonthDesc",
        "CalendarWeekID",
        "CalendarWeekDesc",
        "DayOfWeekNum",
        "DayOfWeekDesc",
        "FiscalYearID",
        "FiscalYearDesc",
        "FiscalQtrID",
        "FiscalQtrDesc",
        "HolidayFlag",
    ],
    parse_dates=["DateValue"],
    dtype={
        "SK_DateID": "uint32",
        "DateDesc": "str",
        "CalendarYearID": "uint16",
        "CalendarYearDesc": "str",
        "CalendarQtrID": "uint16",
        "CalendarQtrDesc": "str",
        "CalendarMonthID": "uint32",
        "CalendarMonthDesc": "str",
        "CalendarWeekID": "uint32",
        "CalendarWeekDesc": "str",
        "DayOfWeekNum": "uint8",
        "DayOfWeekDesc": "str",
        "FiscalYearID": "uint16",
        "FiscalYearDesc": "str",
        "FiscalQtrID": "uint16",
        "FiscalQtrDesc": "str",
        "HolidayFlag": "bool",
    },
)


dtypes = {
    "SK_DateID": sqlalchemy.types.BigInteger,
    "DateValue": sqlalchemy.types.Date,
    "DateDesc": sqlalchemy.types.CHAR(length=20),
    "CalendarYearID": sqlalchemy.types.Integer,
    "CalendarYearDesc": sqlalchemy.types.CHAR(length=20),
    "CalendarQtrID": sqlalchemy.types.Integer,
    "CalendarQtrDesc": sqlalchemy.types.CHAR(length=20),
    "CalendarMonthID": sqlalchemy.types.Integer,
    "CalendarMonthDesc": sqlalchemy.types.CHAR(length=20),
    "CalendarWeekID": sqlalchemy.types.Integer,
    "CalendarWeekDesc": sqlalchemy.types.CHAR(length=20),
    "DayOfWeekNum": sqlalchemy.types.SmallInteger,
    "DayOfWeekDesc": sqlalchemy.types.CHAR(length=10),
    "FiscalYearID": sqlalchemy.types.Integer,
    "FiscalYearDesc": sqlalchemy.types.CHAR(length=20),
    "FiscalQtrID": sqlalchemy.types.Integer,
    "FiscalQtrDesc": sqlalchemy.types.CHAR(length=20),
    "HolidayFlag": sqlalchemy.types.Boolean,
}


create_table = """CREATE TABLE IF NOT EXISTS DimDate (
    SK_DateID INT UNSIGNED NOT NULL,
    DateValue DATE NOT NULL,
    DateDesc CHAR(20) NOT NULL,
    CalendarYearID SMALLINT UNSIGNED NOT NULL,
    CalendarYearDesc CHAR(20) NOT NULL,
    CalendarQtrID SMALLINT UNSIGNED NOT NULL,
    CalendarQtrDesc CHAR(20) NOT NULL,
    CalendarMonthID MEDIUMINT UNSIGNED NOT NULL,
    CalendarMonthDesc CHAR(20) NOT NULL,
    CalendarWeekID MEDIUMINT UNSIGNED NOT NULL,
    CalendarWeekDesc CHAR(20) NOT NULL,
    DayOfWeekNum TINYINT UNSIGNED NOT NULL,
    DayOfWeekDesc CHAR(10) NOT NULL,
    FiscalYearID SMALLINT UNSIGNED NOT NULL,
    FiscalYearDesc CHAR(20) NOT NULL,
    FiscalQtrID SMALLINT UNSIGNED NOT NULL,
    FiscalQtrDesc CHAR(20) NOT NULL,
    HolidayFlag BOOLEAN,
    PRIMARY KEY (SK_DateID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


date_df.to_sql(
    name="dimdate", con=engine, if_exists="append", index=False, dtype=dtypes
)


# ### dimTime


# Read the data from the file
file_path = DATA_DIR + "Time.txt"
dim_time_df = pd.read_csv(
    file_path,
    sep="|",
    header=None,
    names=[
        "SK_TimeID",
        "TimeValue",
        "HourID",
        "HourDesc",
        "MinuteID",
        "MinuteDesc",
        "SecondID",
        "SecondDesc",
        "MarketHoursFlag",
        "OfficeHoursFlag",
    ],
    dtype={
        "SK_TimeID": "uint32",
        "HourID": "uint8",
        "HourDesc": "str",
        "MinuteID": "uint8",
        "MinuteDesc": "str",
        "SecondID": "uint8",
        "SecondDesc": "str",
        "MarketHoursFlag": "bool",
        "OfficeHoursFlag": "bool",
    },
    parse_dates=["TimeValue"],
    date_format="%H:%M:%S",
)
dim_time_df["TimeValue"] = dim_time_df["TimeValue"].dt.time


dtypes = {
    "SK_TimeID": sqlalchemy.types.BigInteger,
    "TimeValue": sqlalchemy.types.Time,
    "HourID": sqlalchemy.types.SmallInteger,
    "HourDesc": sqlalchemy.types.CHAR(length=20),
    "MinuteID": sqlalchemy.types.SmallInteger,
    "MinuteDesc": sqlalchemy.types.CHAR(length=20),
    "SecondID": sqlalchemy.types.SmallInteger,
    "SecondDesc": sqlalchemy.types.CHAR(length=20),
    "MarketHoursFlag": sqlalchemy.types.Boolean,
    "OfficeHoursFlag": sqlalchemy.types.Boolean,
}


create_table = """CREATE TABLE IF NOT EXISTS DimTime (
    SK_TimeID INT UNSIGNED NOT NULL,
    TimeValue TIME(3) NOT NULL,
    HourID TINYINT UNSIGNED NOT NULL,
    HourDesc CHAR(20) NOT NULL,
    MinuteID TINYINT UNSIGNED NOT NULL,
    MinuteDesc CHAR(20) NOT NULL,
    SecondID TINYINT UNSIGNED NOT NULL,
    SecondDesc CHAR(20) NOT NULL,
    MarketHoursFlag BOOLEAN,
    OfficeHoursFlag BOOLEAN,
    PRIMARY KEY (SK_TimeID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))
dim_time_df.to_sql(
    name="dimtime", con=engine, if_exists="append", index=False, dtype=dtypes
)


# ### Industry


# Read the data from the file
file_path = DATA_DIR + "Industry.txt"
industry_df = pd.read_csv(
    file_path,
    sep="|",
    header=None,
    names=["IN_ID", "IN_NAME", "IN_SC_ID"],
    dtype={"IN_ID": "str", "IN_NAME": "str", "IN_SC_ID": "str"},
)


sql_dtypes = {
    "IN_ID": sqlalchemy.types.CHAR(length=2),
    "IN_NAME": sqlalchemy.types.CHAR(length=50),
    "IN_SC_ID": sqlalchemy.types.CHAR(length=4),
}


create_table = """CREATE TABLE IF NOT EXISTS Industry (
    IN_ID CHAR(2) NOT NULL,
    IN_NAME CHAR(50) NOT NULL,
    IN_SC_ID CHAR(4) NOT NULL,
    PRIMARY KEY (IN_ID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


industry_df.to_sql(
    name="industry", con=engine, if_exists="append", index=False, dtype=dtypes
)


# ### StatusType


# Read StatusType data
filepath = DATA_DIR + "StatusType.txt"
status_type_df = pd.read_csv(
    filepath,
    sep="|",
    header=None,
    names=["ST_ID", "ST_NAME"],
    dtype={"ST_ID": "str", "ST_NAME": "str"},
)


sql_dtypes = {
    "ST_ID": sqlalchemy.types.CHAR(length=4),
    "ST_NAME": sqlalchemy.types.CHAR(length=10),
}


with engine.connect() as conn:
    conn.execute(
        text(
            """CREATE TABLE IF NOT EXISTS StatusType (
    ST_ID CHAR(4) NOT NULL,
    ST_NAME CHAR(10) NOT NULL,
    PRIMARY KEY (ST_ID)
);"""
        )
    )


status_type_df.to_sql(
    name="statustype", con=engine, if_exists="append", index=False, dtype=sql_dtypes
)


# ### TradeType


# Read TradeType data
filepath = DATA_DIR + "TradeType.txt"
trade_type_df = pd.read_csv(
    filepath,
    sep="|",
    header=None,
    names=["TT_ID", "TT_NAME", "TT_IS_SELL", "TT_IS_MRKT"],
    dtype={
        "TT_ID": "str",
        "TT_NAME": "str",
        "TT_IS_SELL": "uint8",
        "TT_IS_MRKT": "uint8",
    },
)


dtypes = {
    "TT_ID": sqlalchemy.types.CHAR(length=3),
    "TT_NAME": sqlalchemy.types.CHAR(length=12),
    "TT_IS_SELL": sqlalchemy.types.SmallInteger,
    "TT_IS_MRKT": sqlalchemy.types.SmallInteger,
}
with engine.connect() as conn:
    conn.execute(
        text(
            """CREATE TABLE IF NOT EXISTS TradeType (
    TT_ID CHAR(3) NOT NULL,
    TT_NAME CHAR(12) NOT NULL,
    TT_IS_SELL TINYINT UNSIGNED NOT NULL CHECK (TT_IS_SELL IN (0, 1)),
    TT_IS_MRKT TINYINT UNSIGNED NOT NULL CHECK (TT_IS_MRKT IN (0, 1)),
    PRIMARY KEY (TT_ID)
);"""
        )
    )
trade_type_df.to_sql(
    name="tradetype", con=engine, if_exists="append", index=False, dtype=dtypes
)


# ### TaxRate


# Read TaxRate data
filepath = DATA_DIR + "TaxRate.txt"
tax_rate_df = pd.read_csv(
    filepath,
    sep="|",
    header=None,
    names=["TX_ID", "TX_NAME", "TX_RATE"],
    dtype={"TX_ID": "str", "TX_NAME": "str", "TX_RATE": "float64"},
)


sql_dtypes = {
    "TX_ID": sqlalchemy.types.CHAR(length=4),
    "TX_NAME": sqlalchemy.types.CHAR(length=50),
    "TX_RATE": sqlalchemy.types.Numeric(precision=6, scale=5),
}

with engine.connect() as conn:
    conn.execute(
        text(
            """CREATE TABLE IF NOT EXISTS TaxRate (
    TX_ID CHAR(4) NOT NULL,
    TX_NAME CHAR(50) NOT NULL,
    TX_RATE DECIMAL(6, 5) NOT NULL,
    PRIMARY KEY (TX_ID)
);"""
        )
    )

tax_rate_df.to_sql(
    name="taxrate", con=engine, if_exists="append", index=False, dtype=sql_dtypes
)


# ### dimBroker


hr_df = pd.read_csv(
    DATA_DIR + "HR.csv",
    sep=",",
    header=None,
    names=[
        "EmployeeID",
        "ManagerID",
        "EmployeeFirstName",
        "EmployeeLastName",
        "EmployeeMI",
        "EmployeeJobCode",
        "EmployeeBranch",
        "EmployeeOffice",
        "EmployeePhone",
    ],
    dtype={
        "EmployeeID": "uint32",
        "ManagerID": "uint32",
        "EmployeeFirstName": "str",
        "EmployeeLastName": "str",
        "EmployeeMI": "str",
        "EmployeeJobCode": "UInt16",
        "EmployeeBranch": "str",
        "EmployeeOffice": "str",
        "EmployeePhone": "str",
    },
)


# 1. Filter Records
filtered_df = hr_df[hr_df["EmployeeJobCode"] == 314]
filtered_df.drop(columns=["EmployeeJobCode"], inplace=True)
# 2. Map Columns
DimBroker = filtered_df.rename(
    columns={
        "EmployeeID": "BrokerID",
        "ManagerID": "ManagerID",
        "EmployeeFirstName": "FirstName",
        "EmployeeLastName": "LastName",
        "EmployeeMI": "MiddleInitial",
        "EmployeeBranch": "Branch",
        "EmployeeOffice": "Office",
        "EmployeePhone": "Phone",
    }
)
# 3. Handle Surrogate Key (SK_BrokerID)
# Using cumcount to generate a unique ID for each row
DimBroker.loc[:, "SK_BrokerID"] = range(1, len(DimBroker) + 1)

# 4. Set Default Values for New Fields
DimBroker.loc[:, "IsCurrent"] = True
DimBroker.loc[:, "BatchID"] = BATCH_ID
# EffectiveDate is set to the earliest date in the DimDate table and EndDate is set to 9999- 12-31
DimBroker.loc[:, "EffectiveDate"] = pd.read_sql_query(
    "SELECT MIN(datevalue) FROM dimdate", engine
).iloc[0, 0]
DimBroker.loc[:, "EndDate"] = pd.Timestamp("9999-12-31")


dtypes = {
    "SK_BrokerID": sqlalchemy.types.BigInteger,
    "BrokerID": sqlalchemy.types.BigInteger,
    "ManagerID": sqlalchemy.types.BigInteger,
    "FirstName": sqlalchemy.types.CHAR(length=50),
    "LastName": sqlalchemy.types.CHAR(length=50),
    "MiddleInitial": sqlalchemy.types.CHAR(length=1),
    "Branch": sqlalchemy.types.CHAR(length=50),
    "Office": sqlalchemy.types.CHAR(length=50),
    "Phone": sqlalchemy.types.CHAR(length=14),
    "IsCurrent": sqlalchemy.types.Boolean,
    "BatchID": sqlalchemy.types.Integer,
    "EffectiveDate": sqlalchemy.types.Date,
    "EndDate": sqlalchemy.types.Date,
}


create_table = """CREATE TABLE IF NOT EXISTS DimBroker (
    SK_BrokerID INT UNSIGNED NOT NULL,
    BrokerID INT UNSIGNED NOT NULL,
    ManagerID INT UNSIGNED,
    FirstName CHAR(50) NOT NULL,
    LastName CHAR(50) NOT NULL,
    MiddleInitial CHAR(1),
    Branch CHAR(50),
    Office CHAR(50),
    Phone CHAR(14),
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    PRIMARY KEY (SK_BrokerID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


DimBroker.to_sql(
    name="dimbroker", con=engine, if_exists="append", index=False, dtype=dtypes
)


# ### dimCompany


def read_finwire(file_path):
    # Define the column widths and names
    col_widths = [15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150]
    col_names = [
        "PTS",
        "RecType",
        "CompanyName",
        "CIK",
        "Status",
        "IndustryID",
        "SPrating",
        "FoundingDate",
        "AddrLine1",
        "AddrLine2",
        "PostalCode",
        "City",
        "StateProvince",
        "Country",
        "CEOname",
        "Description",
    ]
    # Read the fixed-width file
    df = pd.read_fwf(file_path, widths=col_widths, header=None, names=col_names)

    # Filter the DataFrame for CMP records
    df_cmp = df[df["RecType"] == "CMP"]
    return df_cmp


# Query StatusType table and create a mapping dictionary
with engine.connect() as conn:
    statustype_df = pd.read_sql("SELECT * FROM statustype", conn)
status_mapping = dict(statustype_df[["ST_ID", "ST_NAME"]].values)

# Query Industry table and create a mapping dictionary
with engine.connect() as conn:
    industry_df = pd.read_sql("SELECT * FROM industry", conn)
industry_mapping = dict(industry_df[["IN_ID", "IN_NAME"]].values)

# Valid SPrating values
valid_spratings = [
    "AAA",
    "AA+",
    "AA",
    "AA-",
    "A+",
    "A",
    "A-",
    "BBB+",
    "BBB",
    "BBB-",
    "BB+",
    "BB",
    "BB-",
    "B+",
    "B",
    "B-",
    "CCC+",
    "CCC",
    "CCC-",
    "CC",
    "C",
    "D",
]


files = os.listdir(DATA_DIR)
finwire_files = [
    file for file in files if file.startswith("FINWIRE") and "audit" not in file
]


sql_dtypes = {
    "SK_CompanyID": sqlalchemy.types.BigInteger,
    "CompanyID": sqlalchemy.types.BigInteger,
    "Status": sqlalchemy.types.CHAR(length=10),
    "Name": sqlalchemy.types.CHAR(length=60),
    "Industry": sqlalchemy.types.CHAR(length=50),
    "SPrating": sqlalchemy.types.CHAR(length=4),
    "isLowGrade": sqlalchemy.types.Boolean,
    "CEO": sqlalchemy.types.CHAR(length=100),
    "AddressLine1": sqlalchemy.types.CHAR(length=80),
    "AddressLine2": sqlalchemy.types.CHAR(length=80),
    "PostalCode": sqlalchemy.types.CHAR(length=12),
    "City": sqlalchemy.types.CHAR(length=25),
    "StateProv": sqlalchemy.types.CHAR(length=20),
    "Country": sqlalchemy.types.CHAR(length=24),
    "Description": sqlalchemy.types.CHAR(length=150),
    "FoundingDate": sqlalchemy.types.Date,
    "IsCurrent": sqlalchemy.types.Boolean,
    "BatchID": sqlalchemy.types.Integer,
    "EffectiveDate": sqlalchemy.types.Date,
    "EndDate": sqlalchemy.types.Date,
}


def load_dimcompany(filename, is_first_batch=False):
    file_path = DATA_DIR + filename
    df_cmp = read_finwire(file_path)

    if len(df_cmp) == 0:
        return

    # Define the column names and data types
    column_names = [
        "SK_CompanyID",
        "CompanyID",
        "Status",
        "Name",
        "Industry",
        "SPrating",
        "isLowGrade",
        "CEO",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
        "City",
        "StateProv",
        "Country",
        "Description",
        "FoundingDate",
        "IsCurrent",
        "BatchID",
        "EffectiveDate",
        "EndDate",
    ]
    dtypes = {
        "SK_CompanyID": "uint32",
        "CompanyID": "uint32",
        "Status": "str",
        "Name": "str",
        "Industry": "str",
        "SPrating": "str",
        "isLowGrade": "boolean",
        "CEO": "str",
        "AddressLine1": "str",
        "AddressLine2": "str",
        "PostalCode": "str",
        "City": "str",
        "StateProv": "str",
        "Country": "str",
        "Description": "str",
        "FoundingDate": "datetime64[ns]",
        "IsCurrent": "bool",
        "BatchID": "uint8",
        "EffectiveDate": "datetime64[ns]",
        "EndDate": "datetime64[ns]",
    }
    # Create an empty DataFrame with the specified schema
    dimCompany = pd.DataFrame(columns=column_names).astype(dtypes)

    # Copy and map relevant columns
    df_cmp["CIK"] = pd.to_numeric(df_cmp["CIK"], downcast="unsigned")
    dimCompany["CompanyID"] = pd.to_numeric(df_cmp["CIK"], downcast="unsigned")
    dimCompany["Name"] = df_cmp["CompanyName"].str.strip()
    dimCompany["SPrating"] = df_cmp["SPrating"].str.upper()
    dimCompany["CEO"] = df_cmp["CEOname"].str.strip()
    dimCompany["Description"] = df_cmp["Description"].str.strip()
    dimCompany["FoundingDate"] = pd.to_datetime(
        df_cmp["FoundingDate"], format="%Y%m%d", errors="coerce"
    )

    # For address fields
    dimCompany["AddressLine1"] = df_cmp["AddrLine1"].str.strip()
    dimCompany["AddressLine2"] = df_cmp["AddrLine2"].str.strip()
    dimCompany["PostalCode"] = df_cmp["PostalCode"].astype(str).str.strip()
    dimCompany["City"] = df_cmp["City"].str.strip()
    dimCompany["StateProv"] = df_cmp["StateProvince"].str.strip()
    dimCompany["Country"] = df_cmp["Country"].str.strip()

    # Replace all-blank strings with None (NULL)
    for col in [
        "Name",
        "SPrating",
        "CEO",
        "Description",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
        "City",
        "StateProv",
        "Country",
    ]:
        dimCompany[col] = dimCompany[col].replace(r"^\s*$", None, regex=True)

    # Update Status in dimCompany
    dimCompany["Status"] = df_cmp["Status"].map(status_mapping)
    # Update Industry in dimCompany
    dimCompany["Industry"] = df_cmp["IndustryID"].map(industry_mapping)
    # isLowGrade is set to False if SPrating begins with ‘A’ or ‘BBB’ otherwise set to True
    dimCompany["isLowGrade"] = ~df_cmp["SPrating"].str.startswith(("A", "BBB"))

    # Identify invalid SPratings
    invalid_sprating_mask = ~dimCompany["SPrating"].isin(valid_spratings)
    # Filter dimCompany for invalid SPrating
    invalid_sprating_data = dimCompany[invalid_sprating_mask]
    if len(invalid_sprating_data) > 0:
        message_data = (
            "CO_ID = "
            + invalid_sprating_data["CompanyID"].astype(str)
            + ", CO_SP_RATE = "
            + invalid_sprating_data["SPrating"]
        )
        # Create DImessages DataFrame
        dimessages = pd.DataFrame(
            {
                "MessageDateAndTime": [datetime.now()] * len(message_data),
                "BatchID": [1] * len(message_data),
                "MessageSource": ["DimCompany"] * len(message_data),
                "MessageText": ["Invalid SPRating"] * len(message_data),
                "MessageType": ["Alert"] * len(message_data),
                "MessageData": message_data,
            }
        )
        # Update dimCompany for invalid SPrating
        dimCompany.loc[invalid_sprating_mask, ["SPrating", "isLowGrade"]] = pd.NA
        # Insert DImessages into MySQL
        dimessages.to_sql("dimessages", engine, if_exists="append", index=False)

    dimCompany.loc[:, "BatchID"] = 1
    dimCompany["EffectiveDate"] = pd.to_datetime(df_cmp["PTS"], format="%Y%m%d-%H%M%S")
    # Identify new and existing records based on CIK
    if is_first_batch:
        new_records = dimCompany
        existing_records = pd.DataFrame(columns=column_names).astype(dtypes)
        next_sk_id = 0
    else:
        existing_cik = pd.read_sql_query(
            "SELECT CompanyID FROM dimCompany WHERE IsCurrent = 1", engine
        )["CompanyID"]
        new_records = dimCompany[~df_cmp["CIK"].isin(existing_cik)]
        existing_records = dimCompany[df_cmp["CIK"].isin(existing_cik)]
        next_sk_id_query = "SELECT MAX(SK_CompanyID) FROM dimCompany"
        next_sk_id = pd.read_sql_query(next_sk_id_query, engine).iloc[0, 0] or 0

    new_records.loc[:, "SK_CompanyID"] = range(
        next_sk_id + 1, next_sk_id + 1 + len(new_records)
    )
    new_records.loc[:, "IsCurrent"] = True
    new_records.loc[:, "EndDate"] = pd.Timestamp("9999-12-31")
    new_records.to_sql(
        "dimcompany", engine, if_exists="append", index=False, dtype=sql_dtypes
    )
    next_sk_id = new_records["SK_CompanyID"].max()

    # Process existing records
    for _, row in existing_records.iterrows():
        effective_date = row["EffectiveDate"]
        company_id = row["CompanyID"]
        # get the current record
        select_query = f"""SELECT * FROM dimCompany
        WHERE CompanyID = '{company_id}' AND IsCurrent = 1"""
        existing_record = pd.read_sql(select_query, engine).iloc[0]
        compare_cols = [
            "Status",
            "Name",
            "Industry",
            "SPrating",
            "isLowGrade",
            "CEO",
            "AddressLine1",
            "AddressLine2",
            "PostalCode",
            "City",
            "StateProv",
            "Country",
            "Description",
            "FoundingDate",
        ]
        is_same = True
        for col in compare_cols:
            if row[col] != existing_record[col]:
                is_same = False
                break
        if not is_same:
            # Expire the current record in MySQL
            update_query = f"""UPDATE dimcompany 
            SET IsCurrent = 0, EndDate = '{effective_date}' 
            WHERE CompanyID = '{company_id}' AND IsCurrent = 1
            """
            with engine.connect() as conn:
                conn.execute(text(update_query))
                conn.commit()
            # Insert updated record
            row["SK_CompanyID"] = next_sk_id + 1
            row["IsCurrent"] = True
            row["EndDate"] = pd.Timestamp("9999-12-31")
            row_df = pd.DataFrame(row).T
            # insert records with existing SK_CompanyID
            row_df.to_sql(
                "dimcompany", engine, if_exists="append", index=False, dtype=sql_dtypes
            )
            next_sk_id += 1

    return df_cmp, new_records, existing_records


create_table = """CREATE TABLE IF NOT EXISTS DimCompany (
    SK_CompanyID INT UNSIGNED NOT NULL,
    CompanyID INT UNSIGNED NOT NULL,
    Status CHAR(10) NOT NULL,
    Name CHAR(60) NOT NULL,
    Industry CHAR(50) NOT NULL,
    SPrating CHAR(4),
    isLowGrade BOOLEAN,
    CEO CHAR(100) NOT NULL,
    AddressLine1 CHAR(80),
    AddressLine2 CHAR(80),
    PostalCode CHAR(12) NOT NULL,
    City CHAR(25) NOT NULL,
    StateProv CHAR(20) NOT NULL,
    Country CHAR(24),
    Description CHAR(150) NOT NULL,
    FoundingDate DATE,
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    PRIMARY KEY (SK_CompanyID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


for i, file in enumerate(tqdm(finwire_files)):
    load_dimcompany(file, i == 0)


# ### Financial


def read_finwire_fin(file_path):
    # Define the column widths and names
    col_widths = [15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60]
    col_names = [
        "PTS",
        "RecType",
        "Year",
        "Quarter",
        "QtrStartDate",
        "PostingDate",
        "Revenue",
        "Earnings",
        "EPS",
        "DilutedEPS",
        "Margin",
        "Inventory",
        "Assets",
        "Liabilities",
        "ShOut",
        "DilutedShOut",
        "CoNameOrCIK",
    ]
    # Read the fixed-width file
    df_fin = pd.read_fwf(file_path, widths=col_widths, header=None, names=col_names)
    # Filter the DataFrame for CMP records
    df_fin = df_fin[df_fin["RecType"] == "FIN"]
    # Convert PTS to datetime
    df_fin["PTS"] = pd.to_datetime(df_fin["PTS"], format="%Y%m%d-%H%M%S")

    return df_fin


def load_financial():
    files = os.listdir(DATA_DIR)
    finwire_files = [
        file for file in files if file.startswith("FINWIRE") and "audit" not in file
    ]

    for filename in tqdm(finwire_files):
        file_path = DATA_DIR + filename
        df_fin = read_finwire_fin(file_path)
        if len(df_fin) == 0:
            continue
        # datatypes for the mysql table
        sql_dtypes = {
            "SK_CompanyID": sqlalchemy.types.BigInteger,
            "FI_YEAR": sqlalchemy.types.Integer,
            "FI_QTR": sqlalchemy.types.SmallInteger,
            "FI_QTR_START_DATE": sqlalchemy.types.Date,
            "FI_REVENUE": sqlalchemy.types.Numeric(precision=15, scale=2),
            "FI_NET_EARN": sqlalchemy.types.Numeric(precision=15, scale=2),
            "FI_BASIC_EPS": sqlalchemy.types.Numeric(precision=10, scale=2),
            "FI_DILUT_EPS": sqlalchemy.types.Numeric(precision=10, scale=2),
            "FI_MARGIN": sqlalchemy.types.Numeric(precision=10, scale=2),
            "FI_INVENTORY": sqlalchemy.types.Numeric(precision=15, scale=2),
            "FI_ASSETS": sqlalchemy.types.Numeric(precision=15, scale=2),
            "FI_LIABILITY": sqlalchemy.types.Numeric(precision=15, scale=2),
            "FI_OUT_BASIC": sqlalchemy.types.BigInteger,
            "FI_OUT_DILUT": sqlalchemy.types.BigInteger,
        }

        # data types for the DataFrame
        dtypes = {
            "SK_CompanyID": "uint32",
            "FI_YEAR": "uint16",
            "FI_QTR": "uint8",
            "FI_QTR_START_DATE": "datetime64[ns]",
            "FI_REVENUE": "float64",
            "FI_NET_EARN": "float64",
            "FI_BASIC_EPS": "float64",
            "FI_DILUT_EPS": "float64",
            "FI_MARGIN": "float64",
            "FI_INVENTORY": "float64",
            "FI_ASSETS": "float64",
            "FI_LIABILITY": "float64",
            "FI_OUT_BASIC": "uint64",
            "FI_OUT_DILUT": "uint64",
        }

        # Create empty DataFrame
        financial_df = pd.DataFrame(
            {col: pd.Series(dtype=typ) for col, typ in dtypes.items()}
        )

        # copy directly
        financial_df["FI_YEAR"] = pd.to_numeric(
            df_fin["Year"], downcast="unsigned"
        )
        financial_df["FI_QTR"] = pd.to_numeric(
            df_fin["Quarter"], downcast="unsigned"
        )
        financial_df["FI_QTR_START_DATE"] = pd.to_datetime(
            df_fin["QtrStartDate"], format="%Y%m%d"
        )
        financial_df["FI_REVENUE"] = pd.to_numeric(
            df_fin["Revenue"], downcast="float"
        )
        financial_df["FI_NET_EARN"] = pd.to_numeric(
            df_fin["Earnings"], downcast="float"
        )
        financial_df["FI_BASIC_EPS"] = pd.to_numeric(
            df_fin["EPS"], downcast="float"
        )
        financial_df["FI_DILUT_EPS"] = pd.to_numeric(
            df_fin["DilutedEPS"], downcast="float"
        )
        financial_df["FI_MARGIN"] = pd.to_numeric(
            df_fin["Margin"], downcast="float"
        )
        financial_df["FI_INVENTORY"] = pd.to_numeric(
            df_fin["Inventory"].str.strip(), downcast="float"
        )
        financial_df["FI_ASSETS"] = pd.to_numeric(
            df_fin["Assets"], downcast="float"
        )
        financial_df["FI_LIABILITY"] = pd.to_numeric(
            df_fin["Liabilities"], downcast="float"
        )
        financial_df["FI_OUT_BASIC"] = pd.to_numeric(
            df_fin["ShOut"], downcast="unsigned"
        )
        financial_df["FI_OUT_DILUT"] = pd.to_numeric(
            df_fin["DilutedShOut"], downcast="unsigned"
        )

        # Split df_fin based on the length of CoNameOrCIK
        df_fin_id = df_fin[df_fin["CoNameOrCIK"].str.len() == 10][
            ["PTS", "CoNameOrCIK"]
        ]
        df_fin_id["PTS"] = df_fin_id["PTS"].dt.strftime("%Y-%m-%d")
        df_fin_id["CoNameOrCIK"] = pd.to_numeric(
            df_fin_id["CoNameOrCIK"], downcast="unsigned"
        )
        df_fin_name = df_fin[df_fin["CoNameOrCIK"].str.len() != 10][
            ["PTS", "CoNameOrCIK"]
        ]
        df_fin_name["PTS"] = df_fin_name["PTS"].dt.strftime("%Y-%m-%d")
        df_fin_name["CoNameOrCIK"] = df_fin_name["CoNameOrCIK"].str.strip()

        def build_query(df, id_or_name_col):
            """Function to build SQL query for date range checks"""
            query_parts = []
            for _, row in df.iterrows():
                pts = row["PTS"]
                if id_or_name_col == "CompanyID":
                    company_id = row["CoNameOrCIK"]
                    query_part = f"(CompanyID = {company_id} AND EffectiveDate <= '{pts}' AND '{pts}' < EndDate)"
                else:  # Name
                    company_name = row["CoNameOrCIK"]
                    query_part = f"(Name = '{company_name}' AND EffectiveDate <= '{pts}' AND '{pts}' < EndDate)"
                query_parts.append(query_part)
            return " OR ".join(query_parts)

        # Execute query and map results for ID-based records
        if not df_fin_id.empty:
            query_id = (
                f"SELECT CompanyID, SK_CompanyID FROM dimcompany WHERE "
                + build_query(df_fin_id, "CompanyID")
            )
            sk_id_map = pd.read_sql_query(query_id, engine).set_index("CompanyID")[
                "SK_CompanyID"
            ]
            financial_df.loc[df_fin_id.index, "SK_CompanyID"] = (
                df_fin_id["CoNameOrCIK"].astype(int).map(sk_id_map)
            )

        # Execute query and map results for Name-based records
        if not df_fin_name.empty:
            query_name = (
                f"SELECT Name, SK_CompanyID FROM dimcompany WHERE "
                + build_query(df_fin_name, "Name")
            )
            sk_name_map = pd.read_sql_query(query_name, engine).set_index("Name")[
                "SK_CompanyID"
            ]
            financial_df.loc[df_fin_name.index, "SK_CompanyID"] = df_fin_name[
                "CoNameOrCIK"
            ].map(sk_name_map)

        financial_df["SK_CompanyID"] = financial_df["SK_CompanyID"].astype("uint32")

        # perform migration
        financial_df.to_sql(
            "financial", engine, if_exists="append", index=False, dtype=sql_dtypes
        )


create_table = """CREATE TABLE IF NOT EXISTS Financial (
    SK_CompanyID INT UNSIGNED NOT NULL,
    FI_YEAR YEAR NOT NULL,
    FI_QTR TINYINT UNSIGNED NOT NULL CHECK (FI_QTR IN (1, 2, 3, 4)),
    FI_QTR_START_DATE DATE NOT NULL,
    FI_REVENUE DECIMAL(15, 2) NOT NULL,
    FI_NET_EARN DECIMAL(15, 2) NOT NULL,
    FI_BASIC_EPS DECIMAL(10, 2) NOT NULL,
    FI_DILUT_EPS DECIMAL(10, 2) NOT NULL,
    FI_MARGIN DECIMAL(10, 2) NOT NULL,
    FI_INVENTORY DECIMAL(15, 2) NOT NULL,
    FI_ASSETS DECIMAL(15, 2) NOT NULL,
    FI_LIABILITY DECIMAL(15, 2) NOT NULL,
    FI_OUT_BASIC BIGINT NOT NULL,
    FI_OUT_DILUT BIGINT NOT NULL,
    PRIMARY KEY (SK_CompanyID, FI_YEAR, FI_QTR)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


load_financial()


# ### dimSecurity


def read_finwire_sec(file_path):
    # Define the column widths and names
    col_widths = [15, 3, 15, 6, 4, 70, 6, 13, 8, 8, 12, 60]
    col_names = [
        "PTS",
        "RecType",
        "Symbol",
        "IssueType",
        "Status",
        "Name",
        "ExID",
        "ShOut",
        "FirstTradeDate",
        "FirstTradeExchg",
        "Dividend",
        "CoNameOrCIK",
    ]
    # Read the fixed-width file
    df_sec = pd.read_fwf(file_path, widths=col_widths, header=None, names=col_names)
    # Filter the DataFrame for CMP records
    df_sec = df_sec[df_sec["RecType"] == "SEC"]
    # Convert date cols to datetime
    df_sec["PTS"] = pd.to_datetime(df_sec["PTS"], format="%Y%m%d-%H%M%S")
    df_sec["FirstTradeDate"] = pd.to_datetime(df_sec["FirstTradeDate"], format="%Y%m%d")
    df_sec["FirstTradeExchg"] = pd.to_datetime(
        df_sec["FirstTradeExchg"], format="%Y%m%d"
    )

    return df_sec


sql_dtypes = {
    "SK_SecurityID": sqlalchemy.types.Integer,
    "Symbol": sqlalchemy.types.String(15),
    "Issue": sqlalchemy.types.String(6),
    "Status": sqlalchemy.types.String(10),
    "Name": sqlalchemy.types.String(70),
    "ExchangeID": sqlalchemy.types.String(6),
    "SK_CompanyID": sqlalchemy.types.Integer,
    "SharesOutstanding": sqlalchemy.types.Integer,
    "FirstTrade": sqlalchemy.types.Date,
    "FirstTradeOnExchange": sqlalchemy.types.Date,
    "Dividend": sqlalchemy.types.Numeric(10, 2),
    "IsCurrent": sqlalchemy.types.Boolean,
    "BatchID": sqlalchemy.types.SmallInteger,
    "EffectiveDate": sqlalchemy.types.Date,
    "EndDate": sqlalchemy.types.Date,
}

dtypes = {
    "SK_SecurityID": "uint32",
    "Symbol": "str",
    "Issue": "str",
    "Status": "str",
    "Name": "str",
    "ExchangeID": "str",
    "SK_CompanyID": "uint32",
    "SharesOutstanding": "uint32",
    "FirstTrade": "datetime64[ns]",
    "FirstTradeOnExchange": "datetime64[ns]",
    "Dividend": "float64",
    "IsCurrent": "bool",
    "BatchID": "uint8",
    "EffectiveDate": "datetime64[ns]",
    "EndDate": "datetime64[ns]",
}


# Query StatusType table and create a mapping dictionary
with engine.connect() as conn:
    statustype_df = pd.read_sql("SELECT * FROM statustype", conn)
status_mapping = dict(statustype_df[["ST_ID", "ST_NAME"]].values)


def build_query(df, id_or_name_col):
    """Function to build SQL query for date range checks"""
    query_parts = []
    for _, row in df.iterrows():
        pts = row["PTS"]
        if id_or_name_col == "CompanyID":
            company_id = row["CoNameOrCIK"]
            query_part = f"(CompanyID = {company_id} AND EffectiveDate <= '{pts}' AND '{pts}' < EndDate)"
        else:  # Name
            company_name = row["CoNameOrCIK"]
            query_part = f"(Name = '{company_name}' AND EffectiveDate <= '{pts}' AND '{pts}' < EndDate)"
        query_parts.append(query_part)
    return " OR ".join(query_parts)


def load_dimsecurity():
    finwire_files = os.listdir(DATA_DIR)
    finwire_files = [
        DATA_DIR + file
        for file in finwire_files
        if file.startswith("FINWIRE") and "audit" not in file
    ]
    for i, file in enumerate(tqdm(finwire_files)):
        # raw data from file
        df_sec = read_finwire_sec(file)
        # dimension table in data warehouse
        security_df = pd.DataFrame(
            {col: pd.Series(dtype=typ) for col, typ in dtypes.items()}
        )
        # copy directly
        security_df["Symbol"] = df_sec["Symbol"]
        security_df["Issue"] = df_sec["IssueType"]
        security_df["Name"] = df_sec["Name"]
        security_df["ExchangeID"] = df_sec["ExID"]
        security_df["SharesOutstanding"] = pd.to_numeric(
            df_sec["ShOut"], downcast="unsigned"
        )
        security_df["FirstTrade"] = df_sec["FirstTradeDate"]
        security_df["FirstTradeOnExchange"] = df_sec["FirstTradeExchg"]
        security_df["Dividend"] = pd.to_numeric(df_sec["Dividend"], downcast="float")
        # Update Status in security_df
        security_df["Status"] = df_sec["Status"].map(status_mapping)
        # BatchID is set to 1
        security_df["BatchID"] = BATCH_ID
        # Split df_sec based on the length of CoNameOrCIK
        df_sec_id = df_sec[df_sec["CoNameOrCIK"].str.len() == 10][
            ["PTS", "CoNameOrCIK"]
        ]
        df_sec_id["PTS"] = df_sec_id["PTS"].dt.strftime("%Y-%m-%d")
        df_sec_id["CoNameOrCIK"] = pd.to_numeric(
            df_sec_id["CoNameOrCIK"], downcast="unsigned"
        )
        df_sec_name = df_sec[df_sec["CoNameOrCIK"].str.len() != 10][
            ["PTS", "CoNameOrCIK"]
        ]
        df_sec_name["PTS"] = df_sec_name["PTS"].dt.strftime("%Y-%m-%d")
        df_sec_name["CoNameOrCIK"] = df_sec_name["CoNameOrCIK"].str.strip()
        # Map results for ID-based records
        if not df_sec_id.empty:
            query_id = (
                f"SELECT CompanyID, SK_CompanyID FROM dimcompany WHERE "
                + build_query(df_sec_id, "CompanyID")
            )
            sk_id_map = pd.read_sql_query(query_id, engine).set_index("CompanyID")[
                "SK_CompanyID"
            ]
            # drop duplicates from the index
            sk_id_map = sk_id_map[~sk_id_map.index.duplicated(keep="last")]
            security_df.loc[df_sec_id.index, "SK_CompanyID"] = (
                df_sec_id["CoNameOrCIK"].astype(int).map(sk_id_map)
            )
        # Map results for Name-based records
        if not df_sec_name.empty:
            query_name = (
                f"SELECT Name, SK_CompanyID FROM dimcompany WHERE "
                + build_query(df_sec_name, "Name")
            )
            sk_name_map = pd.read_sql_query(query_name, engine).set_index("Name")[
                "SK_CompanyID"
            ]
            # drop duplicates from the index
            sk_name_map = sk_name_map[~sk_name_map.index.duplicated(keep="last")]
            security_df.loc[df_sec_name.index, "SK_CompanyID"] = df_sec_name[
                "CoNameOrCIK"
            ].map(sk_name_map)
        # change the type back to uint32
        security_df["SK_CompanyID"] = security_df["SK_CompanyID"].astype("uint32")
        # get effective date from posting date
        security_df["EffectiveDate"] = df_sec["PTS"].dt.strftime("%Y-%m-%d")
        # Identify new and existing records based on Symbol
        is_first_batch = i == 0
        if is_first_batch:
            new_records = security_df
            existing_records = pd.DataFrame(
                {col: pd.Series(dtype=typ) for col, typ in dtypes.items()}
            )
            next_sk_id = 0
        else:
            existing_symbol = pd.read_sql_query(
                "SELECT Symbol FROM dimsecurity WHERE IsCurrent = 1", engine
            )["Symbol"]
            new_records = security_df[~security_df["Symbol"].isin(existing_symbol)]
            existing_records = security_df[security_df["Symbol"].isin(existing_symbol)]
            next_sk_id_query = "SELECT MAX(SK_SecurityID) FROM dimsecurity"
            next_sk_id = pd.read_sql_query(next_sk_id_query, engine).iloc[0, 0] or 0
        # update SK_SecurityID, IsCurrent, EndDate
        new_records.loc[:, "SK_SecurityID"] = range(
            next_sk_id + 1, next_sk_id + 1 + len(new_records)
        )
        new_records.loc[:, "IsCurrent"] = True
        new_records.loc[:, "EndDate"] = pd.Timestamp("9999-12-31")
        # Insert records with new SK_CompanyID
        new_records.to_sql(
            "dimsecurity", engine, if_exists="append", index=False, dtype=sql_dtypes
        )
        next_sk_id = new_records["SK_SecurityID"].max()

        # Process existing records
        for _, row in existing_records.iterrows():
            effective_date = row["EffectiveDate"]
            symbol = row["Symbol"]
            # Expire the current record in MySQL
            update_query = f"""UPDATE dimsecurity 
            SET IsCurrent = 0, EndDate = '{effective_date}' 
            WHERE Symbol = '{symbol}' AND IsCurrent = 1
            """
            with engine.connect() as conn:
                conn.execute(text(update_query))
                conn.commit()
            row["SK_SecurityID"] = next_sk_id + 1
            row["IsCurrent"] = True
            row["EndDate"] = pd.Timestamp("9999-12-31")
            row_df = pd.DataFrame(row).T
            # insert records with existing SK_CompanyID
            row_df.to_sql(
                "dimsecurity", engine, if_exists="append", index=False, dtype=sql_dtypes
            )
            next_sk_id += 1


create_table = """CREATE TABLE IF NOT EXISTS DimSecurity (
    SK_SecurityID INT UNSIGNED NOT NULL,
    Symbol CHAR(15) NOT NULL,
    Issue CHAR(6) NOT NULL,
    Status CHAR(10) NOT NULL,
    Name CHAR(70) NOT NULL,
    ExchangeID CHAR(6) NOT NULL,
    SK_CompanyID INT UNSIGNED NOT NULL,
    SharesOutstanding BIGINT UNSIGNED NOT NULL,
    FirstTrade DATE NOT NULL,
    FirstTradeOnExchange DATE NOT NULL,
    Dividend DECIMAL(10, 2) NOT NULL,
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    PRIMARY KEY (SK_SecurityID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


load_dimsecurity()


# ### Prospect

prospect_start_time = datetime.now()

def read_prospect_file(filepath):
    # Define the column names and their data types
    columns = [
        "AgencyID",
        "LastName",
        "FirstName",
        "MiddleInitial",
        "Gender",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
        "City",
        "State",
        "Country",
        "Phone",
        "Income",
        "NumberCars",
        "NumberChildren",
        "MaritalStatus",
        "Age",
        "CreditRating",
        "OwnOrRentFlag",
        "Employer",
        "NumberCreditCards",
        "NetWorth",
    ]

    # Define the data types for reading the file
    dtypes = {
        "AgencyID": "str",
        "LastName": "str",
        "FirstName": "str",
        "MiddleInitial": "str",
        "Gender": "str",
        "AddressLine1": "str",
        "AddressLine2": "str",
        "PostalCode": "str",
        "City": "str",
        "State": "str",
        "Country": "str",
        "Phone": "str",
        "Income": "Int64",
        "NumberCars": "Int8",
        "NumberChildren": "Int8",
        "MaritalStatus": "str",
        "Age": "Int8",
        "CreditRating": "Int16",
        "OwnOrRentFlag": "str",
        "Employer": "str",
        "NumberCreditCards": "Int8",
        "NetWorth": "Int64",
    }

    # Read the CSV file
    raw_prospect_df = pd.read_csv(filepath, header=None, names=columns, dtype=dtypes)

    return raw_prospect_df


raw_prospect_df = read_prospect_file(DATA_DIR + "Prospect.csv")


not_null_cols = ["LastName", "FirstName", "City", "State"]
for col in not_null_cols:
    raw_prospect_df.loc[raw_prospect_df[col].isna(), col] = ""


dtypes = {
    "AgencyID": "str",
    "SK_RecordDateID": "uint32",
    "SK_UpdateDateID": "uint32",
    "BatchID": "uint16",
    "IsCustomer": "boolean",
    "LastName": "str",
    "FirstName": "str",
    "MiddleInitial": "str",
    "Gender": "str",
    "AddressLine1": "str",
    "AddressLine2": "str",
    "PostalCode": "str",
    "City": "str",
    "State": "str",
    "Country": "str",
    "Phone": "str",
    "Income": "uint32",
    "NumberCars": "uint8",
    "NumberChildren": "uint8",
    "MaritalStatus": "str",
    "Age": "uint8",
    "CreditRating": "uint16",
    "OwnOrRentFlag": "str",
    "Employer": "str",
    "NumberCreditCards": "uint8",
    "NetWorth": "int64",
    "MarketingNameplate": "str",
}

# Create an empty DataFrame with the specified schema
prospect_df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in dtypes.items()})


prospect_df["AgencyID"] = raw_prospect_df["AgencyID"]
prospect_df["LastName"] = raw_prospect_df["LastName"]
prospect_df["FirstName"] = raw_prospect_df["FirstName"]
prospect_df["MiddleInitial"] = raw_prospect_df["MiddleInitial"]
prospect_df["Gender"] = raw_prospect_df["Gender"]
# fix data quality issues
prospect_df["Gender"] = prospect_df["Gender"].str.upper()
mask = ~prospect_df["Gender"].isin(["M", "F"])
prospect_df.loc[mask, "Gender"] = "U"
prospect_df["AddressLine1"] = raw_prospect_df["AddressLine1"]
prospect_df["AddressLine2"] = raw_prospect_df["AddressLine2"]
prospect_df["PostalCode"] = raw_prospect_df["PostalCode"]
prospect_df["City"] = raw_prospect_df["City"]
prospect_df["State"] = raw_prospect_df["State"]
prospect_df["Country"] = raw_prospect_df["Country"]
prospect_df["Phone"] = raw_prospect_df["Phone"]
prospect_df["Income"] = raw_prospect_df["Income"]
prospect_df["NumberCars"] = raw_prospect_df["NumberCars"]
prospect_df["NumberChildren"] = raw_prospect_df["NumberChildren"]
prospect_df["MaritalStatus"] = raw_prospect_df["MaritalStatus"]
prospect_df["MaritalStatus"] = prospect_df["MaritalStatus"].str.upper()
mask = ~prospect_df["MaritalStatus"].isin(["S", "M", "D", "W"])
prospect_df.loc[mask, "MaritalStatus"] = "U"
prospect_df["Age"] = raw_prospect_df["Age"]
prospect_df["CreditRating"] = raw_prospect_df["CreditRating"]
prospect_df["OwnOrRentFlag"] = raw_prospect_df["OwnOrRentFlag"]
prospect_df["OwnOrRentFlag"] = prospect_df["OwnOrRentFlag"].str.upper()
mask = ~prospect_df["OwnOrRentFlag"].isin(["O", "R"])
prospect_df.loc[mask, "OwnOrRentFlag"] = "U"
prospect_df["Employer"] = raw_prospect_df["Employer"]
prospect_df["NumberCreditCards"] = raw_prospect_df["NumberCreditCards"]
prospect_df["NetWorth"] = raw_prospect_df["NetWorth"]


sk_dateid = pd.read_sql_query(
    f"select SK_DateID from dimdate where DateValue = '{BATCH_DATE}'", engine
).iloc[0, 0]
# SK_RecordDateID is set to the DimDate SK_DateID field that corresponds to the Batch Date.
prospect_df["SK_RecordDateID"] = sk_dateid
# SK_UpdateDateID is set to the DimDate SK_DateID field that corresponds to the Batch Date
prospect_df["SK_UpdateDateID"] = sk_dateid


# Define conditions for each tag with null checks
conditions = {
    "HighValue": (prospect_df["NetWorth"].notnull() & prospect_df["Income"].notnull())
    & ((prospect_df["NetWorth"] > 1_000_000) | (prospect_df["Income"] > 200_000)),
    "Expenses": (
        prospect_df["NumberChildren"].notnull()
        & prospect_df["NumberCreditCards"].notnull()
    )
    & ((prospect_df["NumberChildren"] > 3) | (prospect_df["NumberCreditCards"] > 5)),
    "Boomer": prospect_df["Age"].notnull() & (prospect_df["Age"] > 45),
    "MoneyAlert": (
        prospect_df["Income"].notnull()
        & prospect_df["CreditRating"].notnull()
        & prospect_df["NetWorth"].notnull()
    )
    & (
        (prospect_df["Income"] < 50_000)
        | (prospect_df["CreditRating"] < 600)
        | (prospect_df["NetWorth"] < 100_000)
    ),
    "Spender": (
        prospect_df["NumberCars"].notnull() & prospect_df["NumberCreditCards"].notnull()
    )
    & ((prospect_df["NumberCars"] > 3) | (prospect_df["NumberCreditCards"] > 7)),
    "Inherited": (prospect_df["Age"].notnull() & prospect_df["NetWorth"].notnull())
    & ((prospect_df["Age"] < 25) & (prospect_df["NetWorth"] > 1_000_000)),
}

# Apply conditions to assign tags
prospect_df["MarketingNameplate"] = ""
for tag, condition in conditions.items():
    prospect_df["MarketingNameplate"] += np.where(condition, tag + "+", "")

# Remove trailing '+' and replace empty strings with None
prospect_df["MarketingNameplate"] = (
    prospect_df["MarketingNameplate"].str.rstrip("+").replace("", None)
)


# IsCurrent and BatchID are set after processing dimCustomer.


# ### dimCustomer


data_file = DATA_DIR + "CustomerMgmt.xml"
tree = etree.parse(data_file)
namespace = {"tpcdi": "http://www.tpc.org/tpc-di"}


dtypes = {
    "SK_CustomerID": "int32",
    "CustomerID": "int32",
    "TaxID": "str",
    "Status": "str",
    "LastName": "str",
    "FirstName": "str",
    "MiddleInitial": "str",
    "Gender": "str",
    "Tier": "UInt8",
    "DOB": "datetime64[ns]",
    "AddressLine1": "str",
    "AddressLine2": "str",
    "PostalCode": "str",
    "City": "str",
    "StateProv": "str",
    "Country": "str",
    "Phone1": "str",
    "Phone2": "str",
    "Phone3": "str",
    "Email1": "str",
    "Email2": "str",
    "NationalTaxRateDesc": "str",
    "NationalTaxRate": "Float64",
    "LocalTaxRateDesc": "str",
    "LocalTaxRate": "Float64",
    "AgencyID": "str",
    "CreditRating": "UInt16",
    "NetWorth": "Float64",
    "MarketingNameplate": "str",
    "IsCurrent": "boolean",
    "BatchID": "uint8",
    "EffectiveDate": "datetime64[ns]",
    "EndDate": "datetime64[ns]",
}


def format_phone_number(phone_element):
    # Extract components of the phone number
    ctry_code = phone_element.findtext(
        "C_CTRY_CODE", default=None, namespaces=namespace
    )
    area_code = phone_element.findtext(
        "C_AREA_CODE", default=None, namespaces=namespace
    )
    local = phone_element.findtext("C_LOCAL", default=None, namespaces=namespace)
    ext = phone_element.findtext("C_EXT", default=None, namespaces=namespace)

    # Apply transformation rules
    if ctry_code and area_code and local:
        phone = f"+{ctry_code} ({area_code}) {local}"
    elif area_code and local:
        phone = f"({area_code}) {local}"
    elif local:
        phone = local
    else:
        return None

    # Add extension if present
    if ext:
        phone += ext

    return phone


def get_all_tax_info(tax_ids):
    tax_ids_str = "','".join(tax_ids)
    query = (
        f"SELECT TX_ID, TX_NAME, TX_RATE FROM taxrate WHERE TX_ID IN ('{tax_ids_str}')"
    )
    result = pd.read_sql_query(query, engine)
    return result.set_index("TX_ID").to_dict("index")


# Initialize a dictionary to store the latest index of 'UPDCUST' or 'INACT' for each CustomerID
latest_updates = {}

# Get all actions
all_actions = tree.xpath(".//tpcdi:Action", namespaces=namespace)
# NEW actions
new_actions = [action for action in all_actions if action.get("ActionType") == "NEW"]
# UPD actions
upd_actions = [
    action for action in all_actions if action.get("ActionType") == "UPDCUST"
]
# INACT actions
inact_actions = [
    action for action in all_actions if action.get("ActionType") == "INACT"
]

# Preprocess to fill the dictionary
for i, action in enumerate(all_actions):
    if action.get("ActionType") in ["UPDCUST", "INACT"]:
        customer = action.find("Customer", namespaces=namespace)
        customer_id = customer.get("C_ID", None)
        if customer_id:
            latest_updates[int(customer_id)] = i


# Modified has_later_update function
def has_later_update(customer_id, current_index):
    """Check for subsequent 'UPDCUST' or 'INACT' actions for a given CustomerID at current_index"""
    return latest_updates.get(customer_id, -1) > current_index


# Create an empty DataFrame with the specified schema
dimCustomer_df = pd.DataFrame(
    {col: pd.Series(dtype=typ) for col, typ in dtypes.items()}
)

# Initialize lists to store tax IDs for each record
national_tax_ids = []
local_tax_ids = []

# temporary prospect_df for matching
prospect_df_temp = prospect_df[
    [
        "AgencyID",
        "CreditRating",
        "NetWorth",
        "MarketingNameplate",
        "LastName",
        "FirstName",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
    ]
].copy()
prospect_df_temp["LastName"] = prospect_df_temp["LastName"].str.upper()
prospect_df_temp["FirstName"] = prospect_df_temp["FirstName"].str.upper()
prospect_df_temp["AddressLine1"] = prospect_df_temp["AddressLine1"].str.upper()
prospect_df_temp["AddressLine2"] = prospect_df_temp["AddressLine2"].str.upper()
prospect_df_temp["PostalCode"] = prospect_df_temp["PostalCode"].str.upper()


# Initialize lists to store data for NEW actions
data = {
    "CustomerID": [],
    "TaxID": [],
    "LastName": [],
    "FirstName": [],
    "MiddleInitial": [],
    "Tier": [],
    "DOB": [],
    "Gender": [],
    "Email1": [],
    "Email2": [],
    "AddressLine1": [],
    "AddressLine2": [],
    "PostalCode": [],
    "City": [],
    "StateProv": [],
    "Country": [],
    "Phone1": [],
    "Phone2": [],
    "Phone3": [],
    "NationalTaxRateDesc": [],
    "NationalTaxRate": [],
    "LocalTaxRateDesc": [],
    "LocalTaxRate": [],
    "AgencyID": [],
    "CreditRating": [],
    "NetWorth": [],
    "MarketingNameplate": [],
    "EffectiveDate": [],
}

# Iterate through each 'Action' element with ActionType="NEW"
for index, action in enumerate(tqdm(new_actions)):
    customer = action.find("Customer", namespaces=namespace)
    name = customer.find("Name", namespaces=namespace)
    contact_info = customer.find("ContactInfo", namespaces=namespace)
    address = customer.find("Address", namespaces=namespace)
    tax_info = customer.find("TaxInfo", namespaces=namespace)

    customer_id = customer.get("C_ID", None)
    customer_id = int(customer_id) if customer_id else None
    data["CustomerID"].append(customer_id)
    data["TaxID"].append(customer.get("C_TAX_ID", None))
    tier = customer.get("C_TIER", None)
    tier = int(tier) if tier else None
    if tier is not None and tier not in (1, 2, 3):
        """
        A record will be inserted in the DImessages table if a customer's Tier is not one of the valid
        values (1,2,3). The MessageSource is “DimCustomer”, the MessageType is “Alert” and the
        MessageText is “Invalid customer tier”. The MessageData field is “C_ID = ” followed by the
        natural key value of the record, then “, C_TIER = ” and the C_TIER value.
        """
        MessageDateAndTime = pd.Timestamp("now")
        sk_customer_id = len(data["CustomerID"])
        message = f"C_ID = {sk_customer_id}, C_TIER = {tier}"
        message_source = "DimCustomer"
        message_type = "Alert"
        message_text = "Invalid customer tier"
        query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
        VALUES ('{MessageDateAndTime}', {BATCH_ID}, '{message_source}', '{message_text}', '{message_type}', '{message}')"""
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()

    data["Tier"].append(tier)
    dob = customer.get("C_DOB", None)
    dob = pd.to_datetime(dob, format="%Y-%m-%d") if dob else None
    """A record will be reported in the DImessages table if a customer's DOB is invalid. A customer's
    DOB is invalid if DOB < Batch Date - 100 years or DOB > Batch Date (customer is over 100
    years old or born in the future). The MessageSource is “DimCustomer”, the MessageType is
    “Alert” and the MessageText is “DOB out of range”. The MessageData field is “C_ID = ”
    followed by the natural key value of the record, then “, C_DOB = ” and the C_DOB value."""
    if dob and (dob < BATCH_DATE - pd.Timedelta(days=100 * 365) or dob > BATCH_DATE):
        MessageDateAndTime = pd.Timestamp("now")
        batch_id = 1
        sk_customer_id = len(data["CustomerID"])
        message = f"C_ID = {sk_customer_id}, C_DOB = {dob}"
        message_source = "DimCustomer"
        message_type = "Alert"
        message_text = "DOB out of range"
        query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
        VALUES ('{MessageDateAndTime}', {batch_id}, '{message_source}', '{message_text}', '{message_type}', '{message}')"""
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
    data["DOB"].append(dob)
    gender = customer.get("C_GNDR", "U")
    if gender is not None:
        gender = gender.upper()
    gender = "U" if gender not in ("M", "F") else gender
    data["Gender"].append(gender)

    first_name = name.findtext("C_F_NAME", default=None, namespaces=namespace)
    data["FirstName"].append(first_name if first_name else None)
    middle_initial = name.findtext("C_M_NAME", default=None, namespaces=namespace)
    data["MiddleInitial"].append(middle_initial if middle_initial else None)
    last_name = name.findtext("C_L_NAME", default=None, namespaces=namespace)
    data["LastName"].append(last_name if last_name else None)

    prim_email = contact_info.findtext(
        "C_PRIM_EMAIL", default=None, namespaces=namespace
    )
    data["Email1"].append(prim_email if prim_email else None)
    alt_email = contact_info.findtext("C_ALT_EMAIL", default=None, namespaces=namespace)
    data["Email2"].append(alt_email if alt_email else None)
    data["Phone1"].append(
        format_phone_number(contact_info.find("C_PHONE_1", namespaces=namespace))
    )
    data["Phone2"].append(
        format_phone_number(contact_info.find("C_PHONE_2", namespaces=namespace))
    )
    data["Phone3"].append(
        format_phone_number(contact_info.find("C_PHONE_3", namespaces=namespace))
    )

    # Extracting address information
    address_line1 = address.findtext("C_ADLINE1", default=None, namespaces=namespace)
    data["AddressLine1"].append(address_line1 if address_line1 else None)
    address_line2 = address.findtext("C_ADLINE2", default=None, namespaces=namespace)
    data["AddressLine2"].append(address_line2 if address_line2 else None)
    postalcode = address.findtext("C_ZIPCODE", default=None, namespaces=namespace)
    data["PostalCode"].append(postalcode if postalcode else None)
    city = address.findtext("C_CITY", default=None, namespaces=namespace)
    data["City"].append(city if city else None)
    state_prov = address.findtext("C_STATE_PROV", default=None, namespaces=namespace)
    data["StateProv"].append(state_prov if state_prov else None)
    country = address.findtext("C_CTRY", default=None, namespaces=namespace)
    data["Country"].append(country if country else None)

    # Store TX_ID as placeholders
    national_tax_id = tax_info.findtext(
        "C_NAT_TX_ID", default=None, namespaces=namespace
    )
    national_tax_id = national_tax_id if national_tax_id else None
    national_tax_ids.append(national_tax_id)
    local_tax_id = tax_info.findtext("C_LCL_TX_ID", default=None, namespaces=namespace)
    local_tax_id = local_tax_id if local_tax_id else None
    local_tax_ids.append(local_tax_id)

    if not has_later_update(customer_id, index):
        # Find matching prospect record
        match = prospect_df_temp[
            (prospect_df_temp["LastName"] == last_name.upper())
            & (prospect_df_temp["FirstName"] == first_name.upper())
            & (prospect_df_temp["AddressLine1"] == address_line1.upper())
            & (prospect_df_temp["AddressLine2"] == address_line2.upper())
            & (prospect_df_temp["PostalCode"] == postalcode.upper())
        ]
        if not match.empty:
            # Set values from the matching prospect record
            data["AgencyID"].append(match["AgencyID"].iloc[-1])
            data["CreditRating"].append(match["CreditRating"].iloc[-1])
            data["NetWorth"].append(match["NetWorth"].iloc[-1])
            data["MarketingNameplate"].append(match["MarketingNameplate"].iloc[-1])
        else:
            # Set values to NULL
            data["AgencyID"].append(None)
            data["CreditRating"].append(None)
            data["NetWorth"].append(None)
            data["MarketingNameplate"].append(None)
    else:
        # Set values to NULL due to later 'UPDCUST' or 'INACT'
        data["AgencyID"].append(None)
        data["CreditRating"].append(None)
        data["NetWorth"].append(None)
        data["MarketingNameplate"].append(None)
    # history tracking
    data["EffectiveDate"].append(
        pd.to_datetime(action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S")
    )

# Get unique TX_IDs and remove None values
unique_tax_ids = set(national_tax_ids + local_tax_ids) - {None}
all_tax_info = get_all_tax_info(unique_tax_ids)

# map each tax ID to its description and rate
for i in range(len(national_tax_ids)):
    national_info = all_tax_info.get(
        national_tax_ids[i], {"TX_NAME": None, "TX_RATE": None}
    )
    data["NationalTaxRateDesc"].append(national_info["TX_NAME"])
    data["NationalTaxRate"].append(national_info["TX_RATE"])

    local_info = all_tax_info.get(local_tax_ids[i], {"TX_NAME": None, "TX_RATE": None})
    data["LocalTaxRateDesc"].append(local_info["TX_NAME"])
    data["LocalTaxRate"].append(local_info["TX_RATE"])

# Creating DataFrame
dimCustomer_df = pd.concat([dimCustomer_df, pd.DataFrame(data)])
dimCustomer_df["Status"] = "ACTIVE"


def df2dict(df, exclude_columns):
    # Remove the specified columns from the DataFrame
    df_filtered = df.drop(columns=exclude_columns)
    # Convert the filtered DataFrame to a dictionary
    df_dict = df_filtered.to_dict(orient="index")
    # Create a new dictionary that maps CustomerID to a dictionary of column values
    customer_dict = {
        row["CustomerID"]: {col: val for col, val in row.items() if col != "CustomerID"}
        for _, row in df_dict.items()
    }
    return customer_dict


exclude_columns = [
    "SK_CustomerID",
    "IsCurrent",
    "BatchID",
    "EffectiveDate",
    "EndDate",
    "Status",
]
# dictionary to track latest values for each customer
customer_data = df2dict(dimCustomer_df, exclude_columns)


# Initialize lists to store data for NEW actions
data = {col: [] for col in data.keys()}

# Iterate through each 'Action' element with ActionType="UPDCUST"
for index, action in enumerate(upd_actions):
    customer = action.find("Customer", namespaces=namespace)
    name = customer.find("Name", namespaces=namespace)
    contact_info = customer.find("ContactInfo", namespaces=namespace)
    address = customer.find("Address", namespaces=namespace)
    tax_info = customer.find("TaxInfo", namespaces=namespace)

    customer_id = int(customer.get("C_ID", None))
    data["CustomerID"].append(customer_id)

    # Update tax_id
    tax_id = customer.get("C_TAX_ID", None)
    if tax_id is None:
        tax_id = customer_data[customer_id]["TaxID"]
    else:
        customer_data[customer_id]["TaxID"] = tax_id
    data["TaxID"].append(tax_id)
    # Update tier
    tier = customer.get("C_TIER", None)
    tier = int(tier) if tier else None
    if tier is None:
        tier = customer_data[customer_id]["Tier"]
    else:
        customer_data[customer_id]["Tier"] = tier
        if tier is not None and tier not in (1, 2, 3):
            """
            A record will be inserted in the DImessages table if a customer's Tier is not one of the valid
            values (1,2,3). The MessageSource is “DimCustomer”, the MessageType is “Alert” and the
            MessageText is “Invalid customer tier”. The MessageData field is “C_ID = ” followed by the
            natural key value of the record, then “, C_TIER = ” and the C_TIER value.
            """
            MessageDateAndTime = pd.Timestamp("now")
            batch_id = 1
            sk_customer_id = len(data["CustomerID"]) + dimCustomer_df.shape[0]
            message = f"C_ID = {sk_customer_id}, C_TIER = {tier}"
            message_source = "DimCustomer"
            message_type = "Alert"
            message_text = "Invalid customer tier"
            query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
            VALUES ('{MessageDateAndTime}', {batch_id}, '{message_source}', '{message_text}', '{message_type}', '{message}')"""
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
    data["Tier"].append(tier)
    # Update DOB
    dob = customer.get("C_DOB", None)
    dob = pd.to_datetime(dob, format="%Y-%m-%d") if dob else None
    if dob is None:
        dob = customer_data[customer_id]["DOB"]
    else:
        customer_data[customer_id]["DOB"] = dob
        """A record will be reported in the DImessages table if a customer's DOB is invalid. A customer's
        DOB is invalid if DOB < Batch Date - 100 years or DOB > Batch Date (customer is over 100
        years old or born in the future). The MessageSource is “DimCustomer”, the MessageType is
        “Alert” and the MessageText is “DOB out of range”. The MessageData field is “C_ID = ”
        followed by the natural key value of the record, then “, C_DOB = ” and the C_DOB value."""
        if dob and (
            dob < BATCH_DATE - pd.Timedelta(days=100 * 365) or dob > BATCH_DATE
        ):
            MessageDateAndTime = pd.Timestamp("now")
            batch_id = 1
            sk_customer_id = len(data["CustomerID"])
            message = f"C_ID = {sk_customer_id}, C_DOB = {dob}"
            message_source = "DimCustomer"
            message_type = "Alert"
            message_text = "DOB out of range"
            query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
            VALUES ('{MessageDateAndTime}', {batch_id}, '{message_source}', '{message_text}', '{message_type}', '{message}')"""
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
    data["DOB"].append(dob)
    # Update gender
    gender = customer.get("C_GNDR", None)
    if gender is None:
        gender = customer_data[customer_id]["Gender"]
    else:
        gender = gender.upper()
        gender = "U" if gender not in ("M", "F") else gender
        customer_data[customer_id]["Gender"] = gender
    data["Gender"].append(gender)

    # Update first name
    if name is None:
        first_name = customer_data[customer_id]["FirstName"]
        data["FirstName"].append(first_name)
        middle_initial = customer_data[customer_id]["MiddleInitial"]
        data["MiddleInitial"].append(middle_initial)
        last_name = customer_data[customer_id]["LastName"]
        data["LastName"].append(last_name)
    else:
        first_name = name.findtext("C_F_NAME", default=None, namespaces=namespace)
        if first_name is None:
            first_name = customer_data[customer_id]["FirstName"]
        else:
            customer_data[customer_id]["FirstName"] = first_name
        data["FirstName"].append(first_name)
        # Update middle initial
        middle_initial = name.findtext("C_M_NAME", default=None, namespaces=namespace)
        if middle_initial is None:
            middle_initial = customer_data[customer_id]["MiddleInitial"]
        else:
            customer_data[customer_id]["MiddleInitial"] = middle_initial
        data["MiddleInitial"].append(middle_initial)
        # Update last name
        last_name = name.findtext("C_L_NAME", default=None, namespaces=namespace)
        if last_name is None:
            last_name = customer_data[customer_id]["LastName"]
        else:
            customer_data[customer_id]["LastName"] = last_name
        data["LastName"].append(last_name if last_name else None)

    if contact_info is None:
        prim_email = customer_data[customer_id]["Email1"]
        data["Email1"].append(prim_email)
        alt_email = customer_data[customer_id]["Email2"]
        data["Email2"].append(alt_email)
        phone1 = customer_data[customer_id]["Phone1"]
        data["Phone1"].append(phone1)
        phone2 = customer_data[customer_id]["Phone2"]
        data["Phone2"].append(phone2)
        phone3 = customer_data[customer_id]["Phone3"]
        data["Phone3"].append(phone3)
    else:
        # update primary email
        prim_email = contact_info.findtext(
            "C_PRIM_EMAIL", default=None, namespaces=namespace
        )
        if prim_email is None:
            prim_email = customer_data[customer_id]["Email1"]
        else:
            customer_data[customer_id]["Email1"] = prim_email
        data["Email1"].append(prim_email if prim_email else None)
        # update alternate email
        alt_email = contact_info.findtext(
            "C_ALT_EMAIL", default=None, namespaces=namespace
        )
        if alt_email is None:
            alt_email = customer_data[customer_id]["Email2"]
        else:
            customer_data[customer_id]["Email2"] = alt_email
        data["Email2"].append(alt_email if alt_email else None)
        # update phone numbers
        phone1 = contact_info.find("C_PHONE_1", namespaces=namespace)
        if phone1 is None:
            phone1 = customer_data[customer_id]["Phone1"]
        else:
            phone1 = format_phone_number(phone1)
            customer_data[customer_id]["Phone1"] = phone1
        data["Phone1"].append(phone1)
        phone2 = contact_info.find("C_PHONE_2", namespaces=namespace)
        if phone2 is None:
            phone2 = customer_data[customer_id]["Phone2"]
        else:
            phone2 = format_phone_number(phone2)
            customer_data[customer_id]["Phone2"] = phone2
        data["Phone2"].append(phone2)
        phone3 = contact_info.find("C_PHONE_3", namespaces=namespace)
        if phone3 is None:
            phone3 = customer_data[customer_id]["Phone3"]
        else:
            phone3 = format_phone_number(phone3)
            customer_data[customer_id]["Phone3"] = phone3
        data["Phone3"].append(phone3)

    if address is None:
        address_line1 = customer_data[customer_id]["AddressLine1"]
        data["AddressLine1"].append(address_line1)
        address_line2 = customer_data[customer_id]["AddressLine2"]
        data["AddressLine2"].append(address_line2)
        postalcode = customer_data[customer_id]["PostalCode"]
        data["PostalCode"].append(postalcode)
        city = customer_data[customer_id]["City"]
        data["City"].append(city)
        state_prov = customer_data[customer_id]["StateProv"]
        data["StateProv"].append(state_prov)
        country = customer_data[customer_id]["Country"]
        data["Country"].append(country)
    else:
        # Extracting address information
        address_line1 = address.findtext(
            "C_ADLINE1", default=None, namespaces=namespace
        )
        if address_line1 is None:
            address_line1 = customer_data[customer_id]["AddressLine1"]
        else:
            customer_data[customer_id]["AddressLine1"] = address_line1
        data["AddressLine1"].append(address_line1 if address_line1 else None)
        address_line2 = address.findtext(
            "C_ADLINE2", default=None, namespaces=namespace
        )
        if address_line2 is None:
            address_line2 = customer_data[customer_id]["AddressLine2"]
        else:
            customer_data[customer_id]["AddressLine2"] = address_line2
        data["AddressLine2"].append(address_line2 if address_line2 else None)
        postalcode = address.findtext("C_ZIPCODE", default=None, namespaces=namespace)
        if postalcode is None:
            postalcode = customer_data[customer_id]["PostalCode"]
        else:
            customer_data[customer_id]["PostalCode"] = postalcode
        data["PostalCode"].append(postalcode if postalcode else None)
        city = address.findtext("C_CITY", default=None, namespaces=namespace)
        if city is None:
            city = customer_data[customer_id]["City"]
        else:
            customer_data[customer_id]["City"] = city
        data["City"].append(city if city else None)
        state_prov = address.findtext(
            "C_STATE_PROV", default=None, namespaces=namespace
        )
        if state_prov is None:
            state_prov = customer_data[customer_id]["StateProv"]
        else:
            customer_data[customer_id]["StateProv"] = state_prov
        data["StateProv"].append(state_prov if state_prov else None)
        country = address.findtext("C_CTRY", default=None, namespaces=namespace)
        if country is None:
            country = customer_data[customer_id]["Country"]
        else:
            customer_data[customer_id]["Country"] = country
        data["Country"].append(country if country else None)

    # Store TX_ID as placeholders
    if tax_info is None:
        national_tax_rate_desc = customer_data[customer_id]["NationalTaxRateDesc"]
        data["NationalTaxRateDesc"].append(national_tax_rate_desc)
        national_tax_rate = customer_data[customer_id]["NationalTaxRate"]
        data["NationalTaxRate"].append(national_tax_rate)
        local_tax_rate_desc = customer_data[customer_id]["LocalTaxRateDesc"]
        data["LocalTaxRateDesc"].append(local_tax_rate_desc)
        local_tax_rate = customer_data[customer_id]["LocalTaxRate"]
        data["LocalTaxRate"].append(local_tax_rate)
        national_tax_ids.append(None)
        local_tax_ids.append(None)
    else:
        national_tax_id = tax_info.findtext(
            "C_NAT_TX_ID", default=None, namespaces=namespace
        )
        if national_tax_id is None:
            national_tax_rate_desc = customer_data[customer_id]["NationalTaxRateDesc"]
            data["NationalTaxRateDesc"].append(national_tax_rate_desc)
            national_tax_rate = customer_data[customer_id]["NationalTaxRate"]
            data["NationalTaxRate"].append(national_tax_rate)
            national_tax_ids.append(None)
        else:
            result = pd.read_sql(
                f"SELECT TX_NAME, TX_RATE FROM taxrate WHERE TX_ID = '{national_tax_id}'",
                engine,
            )
            national_tax_rate_desc = result.iloc[0, 0]
            national_tax_rate = result.iloc[0, 1]
            customer_data[customer_id]["NationalTaxRateDesc"] = national_tax_rate_desc
            customer_data[customer_id]["NationalTaxRate"] = national_tax_rate
            data["NationalTaxRateDesc"].append(national_tax_rate_desc)
            data["NationalTaxRate"].append(national_tax_rate)
        local_tax_id = tax_info.findtext(
            "C_LCL_TX_ID", default=None, namespaces=namespace
        )
        if local_tax_id is None:
            local_tax_rate_desc = customer_data[customer_id]["LocalTaxRateDesc"]
            data["LocalTaxRateDesc"].append(local_tax_rate_desc)
            local_tax_rate = customer_data[customer_id]["LocalTaxRate"]
            data["LocalTaxRate"].append(local_tax_rate)
        else:
            result = pd.read_sql(
                f"SELECT TX_NAME, TX_RATE FROM taxrate WHERE TX_ID = '{local_tax_id}'",
                engine,
            )
            local_tax_rate_desc = result.iloc[0, 0]
            local_tax_rate = result.iloc[0, 1]
            customer_data[customer_id]["LocalTaxRateDesc"] = local_tax_rate_desc
            customer_data[customer_id]["LocalTaxRate"] = local_tax_rate
            data["LocalTaxRateDesc"].append(local_tax_rate_desc)
            data["LocalTaxRate"].append(local_tax_rate)

    if not has_later_update(customer_id, index):
        # Find matching prospect record
        match = prospect_df[
            (prospect_df_temp["LastName"] == last_name.upper())
            & (prospect_df_temp["FirstName"] == first_name.upper())
            & (prospect_df_temp["AddressLine1"] == address_line1.upper())
            & (prospect_df_temp["AddressLine2"] == address_line2.upper())
            & (prospect_df_temp["PostalCode"] == postalcode.upper())
        ]
        if not match.empty:
            # Set values from the matching prospect record
            data["AgencyID"].append(match["AgencyID"].iloc[-1])
            data["CreditRating"].append(match["CreditRating"].iloc[-1])
            data["NetWorth"].append(match["NetWorth"].iloc[-1])
            data["MarketingNameplate"].append(match["MarketingNameplate"].iloc[-1])
        else:
            # Set values to those in customer_data
            data["AgencyID"].append(customer_data[customer_id]["AgencyID"])
            data["CreditRating"].append(customer_data[customer_id]["CreditRating"])
            data["NetWorth"].append(customer_data[customer_id]["NetWorth"])
            data["MarketingNameplate"].append(
                customer_data[customer_id]["MarketingNameplate"]
            )
    else:
        # Set values to those in customer_data due to later 'UPDCUST' or 'INACT'
        data["AgencyID"].append(customer_data[customer_id]["AgencyID"])
        data["CreditRating"].append(customer_data[customer_id]["CreditRating"])
        data["NetWorth"].append(customer_data[customer_id]["NetWorth"])
        data["MarketingNameplate"].append(
            customer_data[customer_id]["MarketingNameplate"]
        )
    # history tracking
    data["EffectiveDate"].append(
        pd.to_datetime(action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S")
    )

# Creating DataFrame
dimCustomer_df = pd.concat([dimCustomer_df, pd.DataFrame(data)])
dimCustomer_df["Status"] = "ACTIVE"


# Initialize lists to store data for NEW actions
data = {col: [] for col in data.keys()}

# Iterate through each 'Action' element with ActionType="INACT"
for index, action in enumerate(inact_actions):
    customer = action.find("Customer", namespaces=namespace)
    customer_id = int(customer.get("C_ID", None))
    data["CustomerID"].append(customer_id)
    # Copy all fields from customer_data
    for col in data.keys():
        if col in ("CustomerID", "EffectiveDate"):
            continue
        else:
            data[col].append(customer_data[customer_id][col])
    # history tracking
    data["EffectiveDate"].append(
        pd.to_datetime(action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S")
    )

# Creating DataFrame
data_df = pd.DataFrame(data)
data_df["Status"] = "INACTIVE"
dimCustomer_df = pd.concat([dimCustomer_df, data_df])
dimCustomer_df["BatchID"] = 1


dimCustomer_df["SK_CustomerID"] = range(1, len(dimCustomer_df) + 1)


# Sort the DataFrame by CustomerID and EffectiveDate
dimCustomer_df.sort_values(by=["CustomerID", "EffectiveDate"], inplace=True)
# Create a shifted DataFrame
shifted_df = dimCustomer_df.shift(-1)
# Update EndDate: If next row has same CustomerID, use its EffectiveDate; otherwise, use default date
dimCustomer_df["EndDate"] = pd.Timestamp("9999-12-31")
mask = dimCustomer_df["CustomerID"] == shifted_df["CustomerID"]
dimCustomer_df.loc[mask, "EndDate"] = shifted_df.loc[mask, "EffectiveDate"]

# Update IsCurrent: True if next row has different CustomerID or is the last row
dimCustomer_df["IsCurrent"] = ~mask
dimCustomer_df.sort_values(by=["SK_CustomerID"], inplace=True)


sql_dtypes = {
    "SK_CustomerID": sqlalchemy.types.Integer,
    "CustomerID": sqlalchemy.types.Integer,
    "TaxID": sqlalchemy.types.String(20),
    "Status": sqlalchemy.types.String(10),
    "LastName": sqlalchemy.types.String(30),
    "FirstName": sqlalchemy.types.String(30),
    "MiddleInitial": sqlalchemy.types.String(1),
    "Gender": sqlalchemy.types.String(1),
    "Tier": sqlalchemy.types.SmallInteger,
    "DOB": sqlalchemy.types.Date,
    "AddressLine1": sqlalchemy.types.String(80),
    "AddressLine2": sqlalchemy.types.String(80),
    "PostalCode": sqlalchemy.types.String(12),
    "City": sqlalchemy.types.String(25),
    "StateProv": sqlalchemy.types.String(20),
    "Country": sqlalchemy.types.String(24),
    "Phone1": sqlalchemy.types.String(30),
    "Phone2": sqlalchemy.types.String(30),
    "Phone3": sqlalchemy.types.String(30),
    "Email1": sqlalchemy.types.String(50),
    "Email2": sqlalchemy.types.String(50),
    "NationalTaxRateDesc": sqlalchemy.types.String(50),
    "NationalTaxRate": sqlalchemy.types.Numeric(6, 5),
    "LocalTaxRateDesc": sqlalchemy.types.String(50),
    "LocalTaxRate": sqlalchemy.types.Numeric(6, 5),
    "AgencyID": sqlalchemy.types.String(30),
    "CreditRating": sqlalchemy.types.SmallInteger,
    "NetWorth": sqlalchemy.types.Numeric(10),
    "MarketingNameplate": sqlalchemy.types.String(100),
    "IsCurrent": sqlalchemy.types.Boolean,
    "BatchID": sqlalchemy.types.SmallInteger,
    "EffectiveDate": sqlalchemy.types.Date,
    "EndDate": sqlalchemy.types.Date,
}


# cast to int 32
cols = ["SK_CustomerID", "CustomerID", "BatchID"]
for col in cols:
    dimCustomer_df[col] = dimCustomer_df[col].astype("int")
dimCustomer_df["Tier"] = dimCustomer_df["Tier"].astype("UInt8")
dimCustomer_df["NationalTaxRate"] = dimCustomer_df["NationalTaxRate"].astype("float32")
dimCustomer_df["LocalTaxRate"] = dimCustomer_df["LocalTaxRate"].astype("float32")


# Convert DataFrame columns to the appropriate types
dimCustomer_df["SK_CustomerID"] = dimCustomer_df["SK_CustomerID"].astype(np.int32)
dimCustomer_df["CustomerID"] = dimCustomer_df["CustomerID"].astype(np.int32)
dimCustomer_df["Tier"] = dimCustomer_df["Tier"].astype(pd.Int8Dtype())
dimCustomer_df["NationalTaxRate"] = dimCustomer_df["NationalTaxRate"].astype(np.float64)
dimCustomer_df["LocalTaxRate"] = dimCustomer_df["LocalTaxRate"].astype(np.float64)
dimCustomer_df["CreditRating"] = dimCustomer_df["CreditRating"].astype(pd.Int16Dtype())
dimCustomer_df["NetWorth"] = dimCustomer_df["NetWorth"].astype(pd.Float64Dtype())
dimCustomer_df["BatchID"] = dimCustomer_df["BatchID"].astype(np.int16)

# Convert date columns to datetime.date
dimCustomer_df["DOB"] = pd.to_datetime(dimCustomer_df["DOB"]).dt.date
dimCustomer_df["EffectiveDate"] = pd.to_datetime(
    dimCustomer_df["EffectiveDate"]
).dt.date
dimCustomer_df["EndDate"] = pd.to_datetime(dimCustomer_df["EndDate"]).dt.date


create_table = """CREATE TABLE IF NOT EXISTS DimCustomer (
    SK_CustomerID INT UNSIGNED NOT NULL,
    CustomerID INT UNSIGNED NOT NULL,
    TaxID CHAR(20) NOT NULL,
    Status CHAR(10) NOT NULL,
    LastName CHAR(30) NOT NULL,
    FirstName CHAR(30) NOT NULL,
    MiddleInitial CHAR(1),
    Gender CHAR(1),
    Tier TINYINT UNSIGNED,
    DOB DATE NOT NULL,
    AddressLine1 CHAR(80) NOT NULL,
    AddressLine2 CHAR(80),
    PostalCode CHAR(12) NOT NULL,
    City CHAR(25) NOT NULL,
    StateProv CHAR(20) NOT NULL,
    Country CHAR(24),
    Phone1 CHAR(30),
    Phone2 CHAR(30),
    Phone3 CHAR(30),
    Email1 CHAR(50),
    Email2 CHAR(50),
    NationalTaxRateDesc CHAR(50),
    NationalTaxRate DECIMAL(6, 5),
    LocalTaxRateDesc CHAR(50),
    LocalTaxRate DECIMAL(6, 5),
    AgencyID CHAR(30),
    CreditRating SMALLINT UNSIGNED,
    NetWorth DECIMAL(10),
    MarketingNameplate CHAR(100),
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    PRIMARY KEY (SK_CustomerID)
);"""


with engine.connect() as conn:
    conn.execute(text(create_table))


dimCustomer_df.to_sql(
    "dimcustomer", engine, if_exists="append", index=False, dtype=sql_dtypes
)


# Create temporary uppercase columns for merging in both DataFrames
merge_fields = ["FirstName", "LastName", "AddressLine1", "AddressLine2", "PostalCode"]
for field in merge_fields:
    prospect_df[f"temp_{field}"] = prospect_df[field].str.upper()
    dimCustomer_df[f"temp_{field}"] = dimCustomer_df[field].str.upper()

# Filter dimCustomer_df for active and current customers
active_customers = dimCustomer_df[
    (dimCustomer_df["IsCurrent"] == True) & (dimCustomer_df["Status"] == "ACTIVE")
]

# Perform an outer merge on the temporary uppercase fields
temp_merge_fields = [f"temp_{field}" for field in merge_fields]
merged_df = prospect_df.merge(
    active_customers,
    how="left",
    left_on=temp_merge_fields,
    right_on=temp_merge_fields,
    indicator=True,
)

# Update IsCustomer based on whether a match was found
prospect_df["IsCustomer"] = merged_df["_merge"] == "both"

# Clean up by dropping the temporary columns
prospect_df.drop(columns=temp_merge_fields, inplace=True)
dimCustomer_df.drop(columns=temp_merge_fields, inplace=True)


prospect_df["BatchID"] = 1


sql_dtypes = {
    "AgencyID": sqlalchemy.types.CHAR(30),
    "SK_RecordDateID": sqlalchemy.types.Integer,
    "SK_UpdateDateID": sqlalchemy.types.Integer,
    "BatchID": sqlalchemy.types.SmallInteger,
    "IsCustomer": sqlalchemy.types.Boolean,
    "LastName": sqlalchemy.types.CHAR(30),
    "FirstName": sqlalchemy.types.CHAR(30),
    "MiddleInitial": sqlalchemy.types.CHAR(1),
    "Gender": sqlalchemy.types.CHAR(1),
    "AddressLine1": sqlalchemy.types.CHAR(80),
    "AddressLine2": sqlalchemy.types.CHAR(80),
    "PostalCode": sqlalchemy.types.CHAR(12),
    "City": sqlalchemy.types.CHAR(25),
    "State": sqlalchemy.types.CHAR(20),
    "Country": sqlalchemy.types.CHAR(24),
    "Phone": sqlalchemy.types.CHAR(30),
    "Income": sqlalchemy.types.Integer,
    "NumberCars": sqlalchemy.types.SmallInteger,
    "NumberChildren": sqlalchemy.types.SmallInteger,
    "MaritalStatus": sqlalchemy.types.CHAR(1),
    "Age": sqlalchemy.types.SmallInteger,
    "CreditRating": sqlalchemy.types.SmallInteger,
    "OwnOrRentFlag": sqlalchemy.types.CHAR(1),
    "Employer": sqlalchemy.types.CHAR(30),
    "NumberCreditCards": sqlalchemy.types.SmallInteger,
    "NetWorth": sqlalchemy.types.BigInteger,
    "MarketingNameplate": sqlalchemy.types.CHAR(100),
}


create_table = """CREATE TABLE IF NOT EXISTS Prospect (
    AgencyID CHAR(30) NOT NULL,
    SK_RecordDateID INT UNSIGNED NOT NULL,
    SK_UpdateDateID INT UNSIGNED NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    IsCustomer BOOLEAN NOT NULL,
    LastName CHAR(30) NOT NULL,
    FirstName CHAR(30) NOT NULL,
    MiddleInitial CHAR(1),
    Gender CHAR(1) CHECK (Gender IN ('M', 'F', 'U')),
    AddressLine1 CHAR(80),
    AddressLine2 CHAR(80),
    PostalCode CHAR(12),
    City CHAR(25) NOT NULL,
    State CHAR(20) NOT NULL,
    Country CHAR(24),
    Phone CHAR(30),
    Income INT UNSIGNED,
    NumberCars TINYINT UNSIGNED,
    NumberChildren TINYINT UNSIGNED,
    MaritalStatus CHAR(1) CHECK (MaritalStatus IN ('S', 'M', 'D', 'W', 'U')),
    Age TINYINT UNSIGNED,
    CreditRating SMALLINT UNSIGNED,
    OwnOrRentFlag CHAR(1) CHECK (OwnOrRentFlag IN ('O', 'R', 'U')),
    Employer CHAR(30),
    NumberCreditCards TINYINT UNSIGNED,
    NetWorth BIGINT,
    MarketingNameplate CHAR(100),
    PRIMARY KEY (AgencyID, SK_RecordDateID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


prospect_df.to_sql(
    "prospect", engine, if_exists="append", index=False, dtype=sql_dtypes
)
print("Time taken for prospect:", (datetime.now() - prospect_start_time).total_seconds())

# As the Prospect file is processed, the number of source rows is counted. After the last
# row, a “Status” message is written to the DImessages table, with the MessageSource
# “Prospect”, MessageText “Source rows” and the MessageData field containing the
# number of rows.


num_rows = prospect_df.shape[0]
message_type = "Status"
message_source = "Prospect"
message_text = f"Inserted rows"
MessageDateAndTime = pd.Timestamp("now")
batch_id = 1

query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
            VALUES ('{MessageDateAndTime}', {batch_id}, '{message_source}', '{message_text}', '{message_type}', '{num_rows}')"""
with engine.connect() as conn:
    conn.execute(text(query))
    conn.commit()


# ### dimAccount


# Define the schema as a dictionary
schema = {
    "SK_AccountID": "uint32",
    "AccountID": "uint32",
    "SK_BrokerID": "uint32",
    "SK_CustomerID": "uint32",
    "Status": "str",
    "AccountDesc": "str",
    "TaxStatus": "UInt8",
    "IsCurrent": "bool",
    "BatchID": "uint8",
    "EffectiveDate": "datetime64[ns]",
    "EndDate": "datetime64[ns]",
}


data_file = DATA_DIR + "CustomerMgmt.xml"
tree = etree.parse(data_file)
namespace = {"tpcdi": "http://www.tpc.org/tpc-di"}

# Get all actions
all_actions = tree.xpath(".//tpcdi:Action", namespaces=namespace)


# Create an empty DataFrame with the specified schema
dimAccount_df = pd.DataFrame({col: pd.Series(dtype=typ) for col, typ in schema.items()})

# initialize lists to store data
relevant_cols = [
    "AccountID",
    "SK_BrokerID",
    "SK_CustomerID",
    "Status",
    "AccountDesc",
    "TaxStatus",
    "EffectiveDate",
]
data = {col: [] for col in relevant_cols}

# initialize dict to store most recent values for each account of a customer
customer_accounts = dict()

for index, action in enumerate(tqdm(all_actions)):
    customer = action.find("Customer", namespaces=namespace)
    customer_id = int(customer.get("C_ID", None))
    if customer_id not in customer_accounts:
        customer_accounts[customer_id] = dict()
    if action.get("ActionType") in ("NEW", "ADDACCT"):
        accounts = action.findall("Customer/Account", namespaces=namespace)
        for account in accounts:
            # set effective date for this account
            action_ts = pd.to_datetime(
                action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S"
            )
            data["EffectiveDate"].append(action_ts)
            # Customer/Account/@CA_ID
            account_id = account.get("CA_ID", None)
            account_id = int(account_id) if account_id else None
            data["AccountID"].append(account_id)
            # Customer/Account/CA_NAME
            account_desc = account.findtext(
                "CA_NAME", default=None, namespaces=namespace
            )
            data["AccountDesc"].append(account_desc)
            # Customer/Account/@CA_TAX_ST
            tax_status = account.get("CA_TAX_ST", None)
            tax_status = int(tax_status) if tax_status else None
            data["TaxStatus"].append(tax_status)
            #  Customer/Account/CA_B_ID
            broker_id = account.findtext("CA_B_ID", default=None, namespaces=namespace)
            broker_id = int(broker_id) if broker_id else None
            data["SK_BrokerID"].append((broker_id, action_ts))
            data["SK_CustomerID"].append((customer_id, action_ts))
            status = "ACTIVE"
            data["Status"].append(status)
            # update customer_accounts
            customer_accounts[customer_id][account_id] = {
                "AccountDesc": account_desc,
                "TaxStatus": tax_status,
                "SK_BrokerID": (broker_id, action_ts),
                "SK_CustomerID": (customer_id, action_ts),
                "Status": status,
            }
    elif action.get("ActionType") == "UPDACCT":
        accounts = action.findall("Customer/Account", namespaces=namespace)
        for account in accounts:
            # set effective date for this account
            action_ts = pd.to_datetime(
                action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S"
            )
            data["EffectiveDate"].append(action_ts)
            # Customer/Account/@CA_ID
            account_id = account.get("CA_ID", None)
            account_id = int(account_id) if account_id else None
            data["AccountID"].append(account_id)
            # Customer/Account/CA_NAME
            account_desc = account.findtext(
                "CA_NAME", default=None, namespaces=namespace
            )
            if account_desc is None:
                account_desc = customer_accounts[customer_id][account_id]["AccountDesc"]
            else:
                customer_accounts[customer_id][account_id]["AccountDesc"] = account_desc
            data["AccountDesc"].append(account_desc)
            # Customer/Account/@CA_TAX_ST
            tax_status = account.get("CA_TAX_ST", None)
            tax_status = int(tax_status) if tax_status else None
            if tax_status is None:
                tax_status = customer_accounts[customer_id][account_id]["TaxStatus"]
            else:
                customer_accounts[customer_id][account_id]["TaxStatus"] = tax_status
            data["TaxStatus"].append(tax_status)
            #  Customer/Account/CA_B_ID
            broker_id = account.findtext("CA_B_ID", default=None, namespaces=namespace)
            broker_id = int(broker_id) if broker_id else None
            if broker_id is None:
                broker_id = customer_accounts[customer_id][account_id]["SK_BrokerID"][0]
            else:
                customer_accounts[customer_id][account_id]["SK_BrokerID"] = (
                    broker_id,
                    action_ts,
                )
            sk_brokerid = (broker_id, action_ts)
            data["SK_BrokerID"].append(sk_brokerid)
            sk_customer_id = (customer_id, action_ts)
            customer_accounts[customer_id][account_id]["SK_CustomerID"] = sk_customer_id
            data["SK_CustomerID"].append(sk_customer_id)
            status = "ACTIVE"
            data["Status"].append(status)
    elif action.get("ActionType") == "UPDCUST":
        accounts = action.findall("Customer/Account", namespaces=namespace)
        for account in accounts:
            # set effective date for this account
            action_ts = pd.to_datetime(
                action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S"
            )
            data["EffectiveDate"].append(action_ts)
            # Customer/Account/@CA_ID
            account_id = account.get("CA_ID", None)
            account_id = int(account_id) if account_id else None
            data["AccountID"].append(account_id)
            # set all other fields as is
            for col in customer_accounts[customer_id][account_id]:
                if not col.startswith("SK_"):
                    data[col].append(customer_accounts[customer_id][account_id][col])
            broker_id = customer_accounts[customer_id][account_id]["SK_BrokerID"][0]
            customer_accounts[customer_id][account_id]["SK_BrokerID"] = (
                broker_id,
                action_ts,
            )
            sk_brokerid = (broker_id, action_ts)
            data["SK_BrokerID"].append(sk_brokerid)
            sk_customer_id = (customer_id, action_ts)
            customer_accounts[customer_id][account_id]["SK_CustomerID"] = sk_customer_id
            data["SK_CustomerID"].append(sk_customer_id)
    elif action.get("ActionType") in ("INACT", "CLOSEACCT"):
        accounts = action.findall("Customer/Account", namespaces=namespace)
        for account in accounts:
            # set effective date for this account
            action_ts = pd.to_datetime(
                action.get("ActionTS"), format="%Y-%m-%dT%H:%M:%S"
            )
            data["EffectiveDate"].append(action_ts)
            # Customer/Account/@CA_ID
            account_id = account.get("CA_ID", None)
            account_id = int(account_id) if account_id else None
            data["AccountID"].append(account_id)
            # set all other fields as is
            for col in customer_accounts[customer_id][account_id]:
                if col.startswith("SK_"):
                    continue
                elif col != "Status":
                    data[col].append(customer_accounts[customer_id][account_id][col])
                else:
                    data[col].append("INACTIVE")
                    customer_accounts[customer_id][account_id][col] = "INACTIVE"
            broker_id = customer_accounts[customer_id][account_id]["SK_BrokerID"][0]
            customer_accounts[customer_id][account_id]["SK_BrokerID"] = (
                broker_id,
                action_ts,
            )
            sk_brokerid = (broker_id, action_ts)
            data["SK_BrokerID"].append(sk_brokerid)
            sk_customer_id = (customer_id, action_ts)
            customer_accounts[customer_id][account_id]["SK_CustomerID"] = sk_customer_id
            data["SK_CustomerID"].append(sk_customer_id)


# query the database to get all SK_BrokerID
query_parts = [
    f"(BrokerID = {broker_id} AND EffectiveDate <= '{action_ts}' <= EndDate)"
    for broker_id, action_ts in data["SK_BrokerID"]
]
# Joining all conditions with 'OR'
conditions = " OR ".join(query_parts)
query = f"""SELECT BrokerID, EffectiveDate, EndDate, SK_BrokerID 
FROM dimbroker 
WHERE {conditions}"""
result = pd.read_sql_query(query, engine)
for index, pair in enumerate(data["SK_BrokerID"]):
    broker_id, action_ts = pair
    sk_brokerid = result[
        (result["BrokerID"] == broker_id)
        & (result["EffectiveDate"] <= action_ts.date())
        & (action_ts.date() <= result["EndDate"])
    ].iloc[0, 3]
    data["SK_BrokerID"][index] = sk_brokerid

# query the database to get all SK_CustomerID
query_parts = [
    f"(CustomerID = {customer_id} AND EffectiveDate <= '{action_ts}' <= EndDate)"
    for customer_id, action_ts in data["SK_CustomerID"]
]
# Joining all conditions with 'OR'
conditions = " OR ".join(query_parts)
query = f"""SELECT CustomerID, EffectiveDate, EndDate, SK_CustomerID 
FROM dimcustomer 
WHERE {conditions}"""
result = pd.read_sql_query(query, engine)
for index, pair in enumerate(data["SK_CustomerID"]):
    customer_id, action_ts = pair
    sk_brokerid = result[
        (result["CustomerID"] == customer_id)
        & (result["EffectiveDate"] <= action_ts.date())
        & (action_ts.date() <= result["EndDate"])
    ].iloc[0, 3]
    data["SK_CustomerID"][index] = sk_brokerid


for col in data:
    dimAccount_df[col] = data[col]
dimAccount_df["SK_AccountID"] = range(1, len(dimAccount_df) + 1)
dimAccount_df["BatchID"] = 1

# Sort the DataFrame by CustomerID and EffectiveDate
dimAccount_df.sort_values(by=["AccountID", "EffectiveDate"], inplace=True)
# Create a shifted DataFrame
shifted_df = dimAccount_df.shift(-1)
# Update EndDate: If next row has same CustomerID, use its EffectiveDate; otherwise, use default date
dimAccount_df["EndDate"] = pd.Timestamp("9999-12-31")
mask = dimAccount_df["AccountID"] == shifted_df["AccountID"]
dimAccount_df.loc[mask, "EndDate"] = shifted_df.loc[mask, "EffectiveDate"]

# Update IsCurrent: True if next row has different CustomerID or is the last row
dimAccount_df["IsCurrent"] = ~mask
dimAccount_df.sort_values(by=["SK_AccountID"], inplace=True)


sql_dtypes = {
    "SK_AccountID": sqlalchemy.types.Integer,
    "AccountID": sqlalchemy.types.Integer,
    "SK_BrokerID": sqlalchemy.types.Integer,
    "SK_CustomerID": sqlalchemy.types.Integer,
    "Status": sqlalchemy.types.String(10),
    "AccountDesc": sqlalchemy.types.String(50),
    "TaxStatus": sqlalchemy.types.SmallInteger,
    "IsCurrent": sqlalchemy.types.Boolean,
    "BatchID": sqlalchemy.types.SmallInteger,
    "EffectiveDate": sqlalchemy.types.Date,
    "EndDate": sqlalchemy.types.Date,
}


create_table = """CREATE TABLE IF NOT EXISTS DimAccount (
    SK_AccountID INT UNSIGNED NOT NULL,
    AccountID INT UNSIGNED NOT NULL,
    SK_BrokerID INT UNSIGNED NOT NULL,
    SK_CustomerID INT UNSIGNED NOT NULL,
    Status CHAR(10) NOT NULL,
    AccountDesc CHAR(50),
    TaxStatus TINYINT UNSIGNED,
    IsCurrent BOOLEAN NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    PRIMARY KEY (SK_AccountID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


dimAccount_df.to_sql(
    "dimaccount", engine, if_exists="append", index=False, dtype=sql_dtypes
)


# ### dimTrade

trade_start_time = datetime.now()

columns = [
    "T_ID",
    "T_DTS",
    "T_ST_ID",
    "T_TT_ID",
    "T_IS_CASH",
    "T_S_SYMB",
    "T_QTY",
    "T_BID_PRICE",
    "T_CA_ID",
    "T_EXEC_NAME",
    "T_TRADE_PRICE",
    "T_CHRG",
    "T_COMM",
    "T_TAX",
]
dtypes = {
    "T_ID": "uint64",
    "T_DTS": "str",
    "T_ST_ID": "str",
    "T_TT_ID": "str",
    "T_IS_CASH": "bool",
    "T_S_SYMB": "str",
    "T_QTY": "uint32",
    "T_BID_PRICE": "float64",
    "T_CA_ID": "uint32",
    "T_EXEC_NAME": "str",
    "T_TRADE_PRICE": "float64",
    "T_CHRG": "float64",
    "T_COMM": "float64",
    "T_TAX": "float64",
}

# Read the file into a DataFrame
trade_df = pd.read_csv(
    DATA_DIR + "Trade.txt",
    sep="|",
    header=None,
    names=columns,
    dtype=dtypes,
    parse_dates=["T_DTS"],
)
trade_df["T_DTS"] = pd.to_datetime(trade_df["T_DTS"])


columns = ["TH_T_ID", "TH_DTS", "TH_ST_ID"]
dtypes = {"TH_T_ID": "uint64", "TH_DTS": "str", "TH_ST_ID": "str"}
tradehistory_df = pd.read_csv(
    DATA_DIR + "TradeHistory.txt",
    sep="|",
    header=None,
    names=columns,
    dtype=dtypes,
    parse_dates=["TH_DTS"],
)
tradehistory_df["TH_DTS"] = pd.to_datetime(tradehistory_df["TH_DTS"])


trade_merged = tradehistory_df.merge(trade_df, left_on="TH_T_ID", right_on="T_ID")
del tradehistory_df
del trade_df


# Pre-fetch data from related tables
date_mapping = (
    pd.read_sql("SELECT DateValue, SK_DateID FROM dimdate", engine)
    .set_index("DateValue")["SK_DateID"]
    .to_dict()
)
time_mapping = (
    pd.read_sql("SELECT TimeValue, SK_TimeID FROM dimtime", engine)
    .set_index("TimeValue")["SK_TimeID"]
    .to_dict()
)
status_mapping = (
    pd.read_sql("SELECT ST_ID, ST_NAME FROM statustype", engine)
    .set_index("ST_ID")["ST_NAME"]
    .to_dict()
)
trade_type_mapping = (
    pd.read_sql("SELECT TT_ID, TT_NAME FROM tradetype", engine)
    .set_index("TT_ID")["TT_NAME"]
    .to_dict()
)

# Fetching security and account info in one go
security_info = pd.read_sql(
    "SELECT Symbol, SK_SecurityID, SK_CompanyID, EffectiveDate, EndDate FROM dimsecurity",
    engine,
)
security_info["EffectiveDate"] = pd.to_datetime(security_info["EffectiveDate"])
account_info = pd.read_sql(
    "SELECT AccountID, SK_AccountID, SK_CustomerID, SK_BrokerID, EffectiveDate, EndDate FROM dimaccount",
    engine,
)
account_info["EffectiveDate"] = pd.to_datetime(account_info["EffectiveDate"])


# direct copy
trade_merged["TradeID"] = trade_merged["T_ID"]
trade_merged["CashFlag"] = trade_merged["T_IS_CASH"]
trade_merged["Quantity"] = trade_merged["T_QTY"]
trade_merged["BidPrice"] = trade_merged["T_BID_PRICE"]
trade_merged["ExecutedBy"] = trade_merged["T_EXEC_NAME"]
trade_merged["TradePrice"] = trade_merged["T_TRADE_PRICE"]
trade_merged["Fee"] = trade_merged["T_CHRG"]
trade_merged["Commission"] = trade_merged["T_COMM"]
trade_merged["Tax"] = trade_merged["T_TAX"]
trade_merged["Status"] = trade_merged["T_ST_ID"].map(status_mapping)
trade_merged["Type"] = trade_merged["T_TT_ID"].map(trade_type_mapping)


# initially set null
trade_merged["SK_CreateDateID"] = None
trade_merged["SK_CreateTimeID"] = None
trade_merged["SK_CloseDateID"] = None
trade_merged["SK_CloseTimeID"] = None

# now populate
create_mask = (trade_merged["TH_ST_ID"] == "PNDG") | (
    trade_merged["TH_ST_ID"] == "SBMT"
)
trade_merged.loc[create_mask, "SK_CreateDateID"] = trade_merged.loc[
    create_mask, "TH_DTS"
].dt.date.map(date_mapping)
trade_merged.loc[create_mask, "SK_CreateTimeID"] = pd.to_timedelta(
    trade_merged.loc[create_mask, "TH_DTS"].dt.time.astype(str)
).map(time_mapping)
close_mask = (trade_merged["TH_ST_ID"] == "CMPT") | (trade_merged["TH_ST_ID"] == "CNCL")
trade_merged.loc[close_mask, "SK_CloseDateID"] = trade_merged.loc[
    close_mask, "TH_DTS"
].dt.date.map(date_mapping)
trade_merged.loc[close_mask, "SK_CloseTimeID"] = pd.to_timedelta(
    trade_merged.loc[close_mask, "TH_DTS"].dt.time.astype(str)
).map(time_mapping)


trade_merged.rename({"T_S_SYMB": "Symbol"}, axis=1, inplace=True)


trade_merged = pd.merge(
    trade_merged,
    security_info,
    how="left",
    on="Symbol",
)


# drop the rows where TH_DTS < EffectiveDate  or TH_DTS > EndDate
trade_merged = trade_merged[
    (trade_merged["TH_DTS"] >= trade_merged["EffectiveDate"])
    & (trade_merged["TH_DTS"].dt.date < trade_merged["EndDate"])
]


trade_merged = pd.merge(
    trade_merged,
    account_info,
    how="left",
    left_on="T_CA_ID",
    right_on="AccountID",
)
trade_merged = trade_merged[
    (trade_merged["TH_DTS"] >= trade_merged["EffectiveDate_y"])
    & (trade_merged["TH_DTS"].dt.date < trade_merged["EndDate_y"])
]


use_cols = [
    "TradeID",
    "SK_BrokerID",
    "SK_CreateDateID",
    "SK_CreateTimeID",
    "SK_CloseDateID",
    "SK_CloseTimeID",
    "Status",
    "Type",
    "CashFlag",
    "SK_SecurityID",
    "SK_CompanyID",
    "Quantity",
    "BidPrice",
    "SK_CustomerID",
    "SK_AccountID",
    "ExecutedBy",
    "TradePrice",
    "Fee",
    "Commission",
    "Tax",
]
trade_merged = trade_merged[use_cols]


trade_merged = trade_merged.groupby("TradeID").last().reset_index()
trade_merged["BatchID"] = 1

dtypes = {
    "TradeID": "uint32",
    "SK_BrokerID": "UInt32",
    "SK_CreateDateID": "uint32",
    "SK_CreateTimeID": "uint32",
    "SK_CloseDateID": "UInt32",
    "SK_CloseTimeID": "UInt32",
    "Status": "str",
    "Type": "str",
    "CashFlag": "bool",
    "SK_SecurityID": "uint32",
    "SK_CompanyID": "uint32",
    "Quantity": "uint32",
    "BidPrice": "float64",
    "SK_CustomerID": "uint32",
    "SK_AccountID": "uint32",
    "ExecutedBy": "str",
    "TradePrice": "float64",
    "Fee": "float64",
    "Commission": "float64",
    "Tax": "float64",
    "BatchID": "uint8",
}
trade_merged = trade_merged.astype(dtypes)


sql_dtypes = {
    "TradeID": sqlalchemy.types.Integer,
    "SK_BrokerID": sqlalchemy.types.Integer,
    "SK_CreateDateID": sqlalchemy.types.Integer,
    "SK_CreateTimeID": sqlalchemy.types.Integer,
    "SK_CloseDateID": sqlalchemy.types.Integer,
    "SK_CloseTimeID": sqlalchemy.types.Integer,
    "Status": sqlalchemy.types.CHAR(10),
    "Type": sqlalchemy.types.CHAR(12),
    "CashFlag": sqlalchemy.types.Boolean,
    "SK_SecurityID": sqlalchemy.types.Integer,
    "SK_CompanyID": sqlalchemy.types.Integer,
    "Quantity": sqlalchemy.types.Integer,
    "BidPrice": sqlalchemy.types.Numeric(8, 2),
    "SK_CustomerID": sqlalchemy.types.Integer,
    "SK_AccountID": sqlalchemy.types.Integer,
    "ExecutedBy": sqlalchemy.types.CHAR(64),
    "TradePrice": sqlalchemy.types.Numeric(8, 2),
    "Fee": sqlalchemy.types.Numeric(10, 2),
    "Commission": sqlalchemy.types.Numeric(10, 2),
    "Tax": sqlalchemy.types.Numeric(10, 2),
    "BatchID": sqlalchemy.types.SmallInteger,
}


create_table = """CREATE TABLE IF NOT EXISTS DimTrade (
    TradeID INT UNSIGNED NOT NULL,
    SK_BrokerID INT UNSIGNED,
    SK_CreateDateID INT UNSIGNED NOT NULL,
    SK_CreateTimeID INT UNSIGNED NOT NULL,
    SK_CloseDateID INT UNSIGNED,
    SK_CloseTimeID INT UNSIGNED,
    Status CHAR(10) NOT NULL,
    Type CHAR(12) NOT NULL,
    CashFlag BOOLEAN NOT NULL,
    SK_SecurityID INT UNSIGNED NOT NULL,
    SK_CompanyID INT UNSIGNED NOT NULL,
    Quantity MEDIUMINT UNSIGNED NOT NULL,
    BidPrice DECIMAL(8, 2) NOT NULL,
    SK_CustomerID INT UNSIGNED NOT NULL,
    SK_AccountID INT UNSIGNED NOT NULL,
    ExecutedBy CHAR(64) NOT NULL,
    TradePrice DECIMAL(8, 2),
    Fee DECIMAL(10, 2),
    Commission DECIMAL(10, 2),
    Tax DECIMAL(10, 2),
    BatchID SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (TradeID)
);"""
with engine.connect() as conn:
    conn.execute(text(create_table))


from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()
try:
    for i in trange(0, trade_merged.shape[0], 100000):
        trade_merged.iloc[i : i + 100000].to_sql(
            "dimtrade", engine, if_exists="append", index=False, dtype=sql_dtypes
        )
    session.commit()
except:
    session.rollback()
    raise
finally:
    session.close()
    
print("Time taken for dimtrade:", (datetime.now() - trade_start_time).total_seconds())



# Filter the DataFrame
invalid_trades = trade_merged[
    (trade_merged["Commission"].notnull())
    & (
        trade_merged["Commission"]
        > (trade_merged["TradePrice"] * trade_merged["Quantity"])
    )
]

# Create lists without using iterrows
MessageSource = ["DimTrade"] * len(invalid_trades)
MessageType = ["Alert"] * len(invalid_trades)
MessageText = ["Invalid trade commission"] * len(invalid_trades)
MessageData = [
    "T_ID = "
    + invalid_trades["TradeID"].astype(str)
    + ", T_COMM = "
    + invalid_trades["Commission"].astype(str)
]
# Convert MessageData from a list of Series to a list of strings
MessageData = MessageData[0].tolist()


query = f"""INSERT INTO Dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
VALUES """
for i in range(len(MessageSource)):
    query += f"""('{pd.Timestamp("now")}', 1, '{MessageSource[i]}', '{MessageText[i]}', '{MessageType[i]}', '{MessageData[i]}'),"""
with engine.connect() as conn:
    conn.execute(text(query[:-1]))
    conn.commit()


# Filter the DataFrame for invalid trade fees
invalid_fee_trades = trade_merged[
    (trade_merged["Fee"].notnull())
    & (trade_merged["Fee"] > (trade_merged["TradePrice"] * trade_merged["Quantity"]))
]

# Create the required lists
MessageSource = ["DimTrade"] * len(invalid_fee_trades)
MessageType = ["Alert"] * len(invalid_fee_trades)
MessageText = ["Invalid trade fee"] * len(invalid_fee_trades)

# Vectorized operation for MessageData
MessageData = (
    "T_ID = "
    + invalid_fee_trades["TradeID"].astype(str)
    + ", T_CHRG = "
    + invalid_fee_trades["Fee"].astype(str)
)
MessageData = MessageData.tolist()

query = """INSERT INTO Dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData) VALUES """
for i in range(len(MessageSource)):
    query += f"""('{pd.Timestamp("now")}', 1, '{MessageSource[i]}', '{MessageText[i]}', '{MessageType[i]}', '{MessageData[i]}'),"""
with engine.connect() as conn:
    conn.execute(text(query[:-1]))
    conn.commit()


# ### FactCashBalances

fcb_start_time = datetime.now()

cash_txn_df = pd.read_csv(
    DATA_DIR + "CashTransaction.txt",
    sep="|",
    header=None,
    names=["CT_CA_ID", "CT_DTS", "CT_AMT", "CT_NAME"],
    dtype={
        "CT_CA_ID": "uint32",
        "CT_DTS": "str",
        "CT_AMT": "float64",
        "CT_NAME": "str",
    },
    parse_dates=["CT_DTS"],
)
cash_txn_df["CT_DTS"] = pd.to_datetime(cash_txn_df["CT_DTS"])


# SK_CustomerID and SK_AccountID are obtained from DimAccount by matching CT_CA_ID
# with AccountID, where CT_DTS is in the range given by EffectiveDate and EndDate
#


account_info = pd.read_sql(
    "SELECT AccountID, SK_AccountID, SK_CustomerID, EffectiveDate, EndDate FROM dimaccount",
    engine,
)
account_info["EffectiveDate"] = pd.to_datetime(account_info["EffectiveDate"])


cash_txn_df = cash_txn_df.merge(
    account_info,
    how="left",
    left_on="CT_CA_ID",
    right_on="AccountID",
)


cash_txn_df = cash_txn_df[
    (cash_txn_df["CT_DTS"] >= cash_txn_df["EffectiveDate"])
    & (cash_txn_df["CT_DTS"].dt.date < cash_txn_df["EndDate"])
]


# SK_DateID is obtained from DimDate by matching just the date portion of CT_DTS with
# DateValue to return the SK_DateID.


date_info = pd.read_sql("SELECT DateValue, SK_DateID FROM dimdate", engine)
date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])

cash_txn_df["SK_DateID"] = cash_txn_df["CT_DTS"].dt.date.map(
    date_info.set_index("DateValue")["SK_DateID"]
)


# Cash is calculated as the sum of the prior Cash amount for this account plus the sum of all
# CT_AMT values from all transactions in this account on this day. If there is no previous
# FactCashBalances record for the associated account, zero is used. Remember that the net effect of all cash transactions for a given account on a given day is totaled, and only a single record is generated per account that had changes per day.
#
# The procedure used to determine the new Cash total must account for the possibility that a
# new surrogate key is created in DimAccount since the last cash transaction.
#


# Sort the DataFrame by account ID and transaction date
cash_txn_df.sort_values(by=["AccountID", "CT_DTS"], inplace=True)

# Create a new column to store the prior cash amount
cash_txn_df["PriorCash"] = (
    cash_txn_df.groupby("AccountID")["CT_AMT"].cumsum() - cash_txn_df["CT_AMT"]
)
cash_txn_df["PriorCash"].fillna(0, inplace=True)

# Calculate the cash balance
cash_txn_df["Cash"] = cash_txn_df["PriorCash"] + cash_txn_df["CT_AMT"]

# Keep only the last record for each account on each day
cash_txn_df = cash_txn_df.groupby(["AccountID", "SK_DateID"]).last().reset_index()


keep_cols = [
    "SK_CustomerID",
    "SK_AccountID",
    "SK_DateID",
    "Cash",
]
cash_txn_df = cash_txn_df[keep_cols]
cash_txn_df["BatchID"] = 1


create_table = """CREATE TABLE IF NOT EXISTS FactCashBalances (
    SK_CustomerID INT UNSIGNED NOT NULL,
    SK_AccountID INT UNSIGNED NOT NULL,
    SK_DateID INT UNSIGNED NOT NULL,
    Cash DECIMAL(15, 2) NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL
);"""

with engine.connect() as connection:
    connection.execute(text(create_table))


sql_dtypes = {
    "SK_CustomerID": sqlalchemy.types.Integer,
    "SK_AccountID": sqlalchemy.types.Integer,
    "SK_DateID": sqlalchemy.types.Integer,
    "Cash": sqlalchemy.types.DECIMAL(precision=15, scale=2),
    "BatchID": sqlalchemy.types.SmallInteger,
}


for i in trange(0, cash_txn_df.shape[0], 100000):
    cash_txn_df.iloc[i : i + 100000].to_sql(
        "factcashbalances", engine, if_exists="append", index=False, dtype=sql_dtypes
    )
print("Time taken for factcashbalances:", (datetime.now() - fcb_start_time).total_seconds())


# ### FactHoldings


# SQL queries
sql_commands = [
    "DROP TABLE IF EXISTS TempHoldingHistory",
    """
    CREATE TEMPORARY TABLE TempHoldingHistory (
        HH_H_T_ID INT UNSIGNED NOT NULL,
        HH_T_ID INT UNSIGNED NOT NULL,
        HH_BEFORE_QTY INT NOT NULL,
        HH_AFTER_QTY INT NOT NULL
    )
    """,
    f"""
    LOAD DATA LOCAL INFILE 'E:\\\\Documents\\\\BDMA\\\\ULB\\\\Data Warehouses\\\\tpc-di\\\\TPC-DI\\\\data\\\\sf{sf}\\\\Batch1\\\\HoldingHistory.txt'
    INTO TABLE TempHoldingHistory
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n'
    (HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY)
    """,
    "DROP TABLE IF EXISTS FactHoldings",
    """
    CREATE TABLE IF NOT EXISTS FactHoldings (
        TradeID INT UNSIGNED NOT NULL,
        CurrentTradeID INT UNSIGNED NOT NULL,
        SK_CustomerID INT UNSIGNED NOT NULL,
        SK_AccountID INT UNSIGNED NOT NULL,
        SK_SecurityID INT UNSIGNED NOT NULL,
        SK_CompanyID INT UNSIGNED NOT NULL,
        SK_DateID INT UNSIGNED NOT NULL,
        SK_TimeID INT UNSIGNED NOT NULL,
        CurrentPrice DECIMAL(8, 2) NOT NULL CHECK (CurrentPrice > 0),
        CurrentHolding INT NOT NULL,
        BatchID SMALLINT UNSIGNED NOT NULL
    )
    """,
    """
    INSERT INTO FactHoldings (TradeID, CurrentTradeID, SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID, SK_DateID, SK_TimeID, CurrentPrice, CurrentHolding, BatchID)
    SELECT 
        thh.HH_H_T_ID AS TradeID,
        thh.HH_T_ID AS CurrentTradeID,
        dt.SK_CustomerID,
        dt.SK_AccountID,
        dt.SK_SecurityID,
        dt.SK_CompanyID,
        dt.SK_CloseDateID AS SK_DateID,
        dt.SK_CloseTimeID AS SK_TimeID,
        dt.TradePrice AS CurrentPrice,
        thh.HH_AFTER_QTY AS CurrentHolding,
        1 AS BatchID
    FROM 
        TempHoldingHistory thh
    JOIN 
        DimTrade dt ON thh.HH_T_ID = dt.TradeID
    """,
    "DROP TABLE IF EXISTS TempHoldingHistory",
]

fh_start_time = datetime.now()
# Executing the queries
with engine.connect() as connection:
    # connection.execute(text("SET GLOBAL local_infile = 1;"))
    for sql in sql_commands:
        connection.execute(text(sql))
    connection.commit()
print("Time taken for factholdings:", (datetime.now() - fh_start_time).total_seconds())


# ### FactMarketHistory

fmh_start_time = datetime.now()

dailymarket_df = pd.read_csv(
    DATA_DIR + "DailyMarket.txt",
    sep="|",
    header=None,
    names=[
        "DM_DATE",
        "DM_S_SYMB",
        "DM_CLOSE",
        "DM_HIGH",
        "DM_LOW",
        "DM_VOL",
    ],
    dtype={
        "DM_DATE": "str",
        "DM_S_SYMB": "str",
        "DM_CLOSE": "float32",
        "DM_HIGH": "float32",
        "DM_LOW": "float32",
        "DM_VOL": "int64",
    },
    parse_dates=["DM_DATE"],
)
dailymarket_df["DM_DATE"] = pd.to_datetime(dailymarket_df["DM_DATE"])


# ClosePrice, DayHigh, DayLow, and Volume are copied from DM_CLOSE, DM_HIGH,
# DM_LOW, and DM_VOL respectively.
dailymarket_df["ClosePrice"] = dailymarket_df["DM_CLOSE"]
dailymarket_df["DayHigh"] = dailymarket_df["DM_HIGH"]
dailymarket_df["DayLow"] = dailymarket_df["DM_LOW"]
dailymarket_df["Volume"] = dailymarket_df["DM_VOL"]


security_info = pd.read_sql(
    "SELECT Symbol AS DM_S_SYMB, SK_SecurityID, SK_CompanyID, EffectiveDate, EndDate FROM dimsecurity",
    engine,
)
security_info["EffectiveDate"] = pd.to_datetime(security_info["EffectiveDate"])


dailymarket_df = pd.merge(
    dailymarket_df,
    security_info,
    how="left",
    on="DM_S_SYMB",
)
dailymarket_df = dailymarket_df[
    (dailymarket_df["DM_DATE"] >= dailymarket_df["EffectiveDate"])
    & (dailymarket_df["DM_DATE"].dt.date < dailymarket_df["EndDate"])
]
# drop temp columns
dailymarket_df.drop(columns=["EffectiveDate", "EndDate"], inplace=True)


"""SK_DateID is obtained from DimDate by matching DM_DATE with DateValue to return the
SK_DateID. The match is guaranteed to succeed because DimDate has been populated
with date information for all dates relevant to the benchmark."""
date_info = pd.read_sql("SELECT DateValue, SK_DateID FROM dimdate", engine)
date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])
date_info = date_info.set_index("DateValue")["SK_DateID"].to_dict()
dailymarket_df["SK_DateID"] = dailymarket_df["DM_DATE"].dt.date.map(date_info)


# Step 2: Sort the DataFrame
dailymarket_df.sort_values(by="DM_DATE", inplace=True)

# Step 3 & 4: Group by 'DM_S_SYMB' and apply rolling max
rolling_max = (
    dailymarket_df.groupby("DM_S_SYMB").rolling("365D", on="DM_DATE")["DM_HIGH"].max()
)

# Reset index to make merging easier
rolling_max = rolling_max.reset_index()

# Step 5: Merge with the original DataFrame
dailymarket_df = dailymarket_df.merge(
    rolling_max, on=["DM_S_SYMB", "DM_DATE"], suffixes=("", "_52WeekHigh")
)

# Rename the column for clarity
dailymarket_df.rename(columns={"DM_HIGH_52WeekHigh": "FiftyTwoWeekHigh"}, inplace=True)

rolling_rank = (
    dailymarket_df.groupby("DM_S_SYMB")
    .rolling("365D", on="DM_DATE")["DM_HIGH"]
    .rank(method="average", ascending=False)
    .reset_index()
    .rename(columns={"DM_HIGH": "Rank"})
)
rolling_rank["Rank"] = rolling_rank["Rank"].astype("uint32")
# Apply the mask to select DM_DATE only for those rows, then forward fill
mask = rolling_rank["Rank"] == 1
rolling_rank["SK_FiftyTwoWeekHighDate"] = rolling_rank["DM_DATE"].where(mask).ffill()
rolling_rank["SK_FiftyTwoWeekHighDate"] = rolling_rank[
    "SK_FiftyTwoWeekHighDate"
].dt.date.map(date_info)

dailymarket_df = pd.concat(
    [dailymarket_df, rolling_rank["SK_FiftyTwoWeekHighDate"]], axis=1
)


dailymarket_df.sort_values(by="DM_DATE", inplace=True)
# Step 3 & 4: Group by 'DM_S_SYMB' and apply rolling min
rolling_min = (
    dailymarket_df.groupby("DM_S_SYMB").rolling("365D", on="DM_DATE")["DM_LOW"].min()
)
# Reset index to make merging easier
rolling_min = rolling_min.reset_index()
# Step 5: Merge with the original DataFrame
dailymarket_df = dailymarket_df.merge(
    rolling_min, on=["DM_S_SYMB", "DM_DATE"], suffixes=("", "_52WeekLow")
)
# Rename the column for clarity
dailymarket_df.rename(columns={"DM_LOW_52WeekLow": "FiftyTwoWeekLow"}, inplace=True)

rolling_rank = (
    dailymarket_df.groupby("DM_S_SYMB")
    .rolling("365D", on="DM_DATE")["DM_LOW"]
    .rank(method="average", ascending=True)
    .reset_index()
    .rename(columns={"DM_LOW": "Rank"})
)
rolling_rank["Rank"] = rolling_rank["Rank"].astype("uint32")
# Apply the mask to select DM_DATE only for those rows, then forward fill
mask = rolling_rank["Rank"] == 1
rolling_rank["SK_FiftyTwoWeekLowDate"] = rolling_rank["DM_DATE"].where(mask).ffill()
rolling_rank["SK_FiftyTwoWeekLowDate"] = rolling_rank[
    "SK_FiftyTwoWeekLowDate"
].dt.date.map(date_info)


dailymarket_df = pd.concat(
    [dailymarket_df, rolling_rank["SK_FiftyTwoWeekLowDate"]], axis=1
)


dailymarket_df["SK_SecurityID"] = dailymarket_df["SK_SecurityID"].astype("uint32")
dailymarket_df["SK_CompanyID"] = dailymarket_df["SK_CompanyID"].astype("uint32")
dailymarket_df["SK_DateID"] = dailymarket_df["SK_DateID"].astype("uint32")
dailymarket_df["FiftyTwoWeekHigh"] = dailymarket_df["FiftyTwoWeekHigh"].astype(
    "float32"
)
dailymarket_df["SK_FiftyTwoWeekHighDate"] = dailymarket_df[
    "SK_FiftyTwoWeekHighDate"
].astype("uint32")
dailymarket_df["FiftyTwoWeekLow"] = dailymarket_df["FiftyTwoWeekLow"].astype("float32")
dailymarket_df["SK_FiftyTwoWeekLowDate"] = dailymarket_df[
    "SK_FiftyTwoWeekLowDate"
].astype("uint32")
dailymarket_df["DM_S_SYMB"] = dailymarket_df["DM_S_SYMB"].astype("category")

dailymarket_df.drop(columns=["DM_HIGH", "DM_LOW", "DM_VOL", "DM_CLOSE"], inplace=True)


security_info = pd.read_sql(
    "SELECT Symbol, Dividend, EffectiveDate, EndDate FROM dimsecurity", engine
)
security_info["EffectiveDate"] = pd.to_datetime(security_info["EffectiveDate"])


dailymarket_df = dailymarket_df.merge(
    security_info,
    how="left",
    left_on="DM_S_SYMB",
    right_on="Symbol",
)
dailymarket_df = dailymarket_df[
    (dailymarket_df["DM_DATE"] >= dailymarket_df["EffectiveDate"])
    & (dailymarket_df["DM_DATE"].dt.date < dailymarket_df["EndDate"])
]
dailymarket_df.drop(columns=["Symbol", "EffectiveDate", "EndDate"], inplace=True)


dailymarket_df["Yield"] = (
    dailymarket_df["Dividend"] / dailymarket_df["ClosePrice"] * 100
)
dailymarket_df.drop(columns=["Dividend"], inplace=True)
dailymarket_df["BatchID"] = 1


sql_dtypes = {
    "SK_SecurityID": sqlalchemy.types.Integer,
    "SK_CompanyID": sqlalchemy.types.Integer,
    "SK_DateID": sqlalchemy.types.Integer,
    "Yield": sqlalchemy.types.DECIMAL(precision=5, scale=2),
    "FiftyTwoWeekHigh": sqlalchemy.types.DECIMAL(precision=8, scale=2),
    "SK_FiftyTwoWeekHighDate": sqlalchemy.types.Integer,
    "FiftyTwoWeekLow": sqlalchemy.types.DECIMAL(precision=8, scale=2),
    "SK_FiftyTwoWeekLowDate": sqlalchemy.types.Integer,
    "ClosePrice": sqlalchemy.types.DECIMAL(precision=8, scale=2),
    "DayHigh": sqlalchemy.types.DECIMAL(precision=8, scale=2),
    "DayLow": sqlalchemy.types.DECIMAL(precision=8, scale=2),
    "Volume": sqlalchemy.types.BigInteger,
    "BatchID": sqlalchemy.types.SmallInteger,
    "DM_DATE": sqlalchemy.types.Date,
    "DM_S_SYMB": sqlalchemy.types.CHAR(16),
}


for i in trange(0, dailymarket_df.shape[0], 100000):
    dailymarket_df.iloc[i : i + 100000].to_sql(
        "tempfactmarketprice", engine, if_exists="append", index=False, dtype=sql_dtypes
    )


sql_commands = [
    "CREATE INDEX idx_sk_companyid ON tempfactmarketprice(SK_CompanyID);",
    """CREATE TABLE IF NOT EXISTS FactMarketHistory (
        SK_SecurityID INT UNSIGNED NOT NULL,
        SK_CompanyID INT UNSIGNED NOT NULL,
        SK_DateID INT UNSIGNED NOT NULL,
        PERatio DECIMAL(10, 2),
        Yield DECIMAL(5, 2) NOT NULL,
        FiftyTwoWeekHigh DECIMAL(8, 2) NOT NULL,
        SK_FiftyTwoWeekHighDate INT UNSIGNED NOT NULL,
        FiftyTwoWeekLow DECIMAL(8, 2) NOT NULL,
        SK_FiftyTwoWeekLowDate INT UNSIGNED NOT NULL,
        ClosePrice DECIMAL(8, 2) NOT NULL,
        DayHigh DECIMAL(8, 2) NOT NULL,
        DayLow DECIMAL(8, 2) NOT NULL,
        Volume BIGINT UNSIGNED NOT NULL,
        BatchID SMALLINT UNSIGNED NOT NULL
    );""",
    "ALTER TABLE factmarkethistory ADD COLUMN DM_S_SYMB TEXT;",
    """INSERT INTO factmarkethistory
        SELECT SK_SecurityID, fmp.SK_CompanyID, SK_DateID, fmp.ClosePrice / T.Sum_EPS AS PERatio, Yield, FiftyTwoWeekHigh,
        SK_FiftyTwoWeekHighDate, FiftyTwoWeekLow, SK_FiftyTwoWeekLowDate, ClosePrice, DayHigh, DayLow, Volume, BatchID, DM_S_SYMB
        FROM tempfactmarketprice fmp
        LEFT JOIN (SELECT 
            c.CompanyID, 
            c.SK_CompanyID AS SKCID, 
            f.FI_QTR_START_DATE,
            SUM(f.FI_BASIC_EPS) OVER (
                PARTITION BY c.CompanyID 
                ORDER BY f.FI_QTR_START_DATE 
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) AS Sum_EPS
        FROM financial f RIGHT JOIN dimCompany c ON f.SK_CompanyID = c.SK_CompanyID
        ORDER BY c.CompanyID, f.FI_QTR_START_DATE) T
        ON T.SKCID = fmp.SK_CompanyID
        AND T.FI_QTR_START_DATE < fmp.DM_DATE 
        AND T.FI_QTR_START_DATE >= DATE_SUB(fmp.DM_DATE, INTERVAL 3 MONTH);""",
    """INSERT INTO dimessages
        SELECT NOW() AS MessageDateAndTime, 1 AS BATCHID, 'FactMarketHistory' AS MessageSource, 'No earnings for company' AS MessageText,
        'Alert' AS MessageType, CONCAT('DM_S_SYMB = ', DM_S_SYMB)
        FROM factmarkethistory 
        WHERE PERatio IS NULL;""",
    "ALTER TABLE factmarkethistory DROP COLUMN DM_S_SYMB;",
    "DROP TABLE tempfactmarketprice;",
]


# Executing the queries
with engine.connect() as connection:
    for sql in sql_commands:
        connection.execute(text(sql))
print("Time taken for factmarkethistory:", (datetime.now() - fmh_start_time).total_seconds())


# ### FactWatches

fw_start_time = datetime.now()

df = pd.read_csv(
    DATA_DIR + "WatchHistory.txt",
    sep="|",
    header=None,
    names=["W_C_ID", "W_S_SYMB", "W_DTS", "W_ACTION"],
    dtype={"W_C_ID": "uint32", "W_S_SYMB": "str", "W_DTS": "str", "W_ACTION": "str"},
    parse_dates=["W_DTS"],
)


customer_info = pd.read_sql_query(
    "SELECT CustomerID, SK_CustomerID, EffectiveDate, EndDate FROM dimcustomer", engine
)
customer_info["EffectiveDate"] = pd.to_datetime(customer_info["EffectiveDate"])
security_info = pd.read_sql_query(
    "SELECT Symbol, SK_SecurityID, EffectiveDate, EndDate FROM dimsecurity", engine
)
security_info["EffectiveDate"] = pd.to_datetime(security_info["EffectiveDate"])
date_info = pd.read_sql_query("SELECT DateValue, SK_DateID FROM dimdate", engine)
date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])


# get SK_CustomerID
df = df.merge(customer_info, how="left", left_on="W_C_ID", right_on="CustomerID")
# filter based on date
df = df[(df["W_DTS"] >= df["EffectiveDate"]) & (df["W_DTS"].dt.date < df["EndDate"])]
# drop cols
df.drop(columns=["CustomerID", "EffectiveDate", "EndDate"], inplace=True)
# get SK_SecurityID
df = df.merge(security_info, how="left", left_on="W_S_SYMB", right_on="Symbol")
# filter based on date
df = df[(df["W_DTS"] >= df["EffectiveDate"]) & (df["W_DTS"].dt.date < df["EndDate"])]
# drop cols
df.drop(columns=["Symbol", "EffectiveDate", "EndDate"], inplace=True)
# SK_DateID_DatePlaced - set based on W_DTS.
df["SK_DateID_DatePlaced"] = df["W_DTS"].dt.date.map(
    date_info.set_index("DateValue")["SK_DateID"]
)
# BatchID - set to 1.
df["BatchID"] = 1


# Mask for rows where W_ACTION is 'CNCL'
mask_cncl = df["W_ACTION"] == "CNCL"
df.loc[mask_cncl, "SK_DateID_DateRemoved"] = df.loc[mask_cncl, "W_DTS"].dt.date.map(
    date_info.set_index("DateValue")["SK_DateID"]
)
df.loc[~mask_cncl, "SK_DateID_DateRemoved"] = None


keep_cols = [
    "SK_CustomerID",
    "SK_SecurityID",
    "SK_DateID_DatePlaced",
    "SK_DateID_DateRemoved",
    "BatchID",
]
df = df.groupby(["W_C_ID", "W_S_SYMB"]).first().reset_index()[keep_cols]


sql_dtypes = {
    "SK_CustomerID": sqlalchemy.types.Integer,
    "SK_SecurityID": sqlalchemy.types.Integer,
    "SK_DateID_DatePlaced": sqlalchemy.types.Integer,
    "SK_DateID_DateRemoved": sqlalchemy.types.Integer,
    "BatchID": sqlalchemy.types.SmallInteger,
}


create_table = """CREATE TABLE IF NOT EXISTS FactWatches (
    SK_CustomerID INT UNSIGNED NOT NULL,
    SK_SecurityID INT UNSIGNED NOT NULL,
    SK_DateID_DatePlaced INT UNSIGNED NOT NULL,
    SK_DateID_DateRemoved INT UNSIGNED,
    BatchID SMALLINT UNSIGNED NOT NULL
);"""

with engine.connect() as connection:
    connection.execute(text(create_table))


for i in trange(0, df.shape[0], 100000):
    df.iloc[i : i + 100000].to_sql(
        "factwatches", engine, if_exists="append", index=False, dtype=sql_dtypes
    )
print("Time taken for factwatches:", (datetime.now() - fw_start_time).total_seconds())


# ### Batch Validation

fk_start_time = datetime.now()
filepath = r"scripts\fk.sql"
# Read the SQL file
with open(filepath, "r") as file:
    sql_file = file.read()
queries = sql_file.split(";")

# Create foreign keys to speed up batch validation queries
with engine.connect() as connection:
    for query in tqdm(queries):
        query = query.strip() + ";"
        if len(query) < 5:
            continue
        connection.execute(text(query))
        
print("Time taken for foreign keys:", (datetime.now() - fk_start_time).total_seconds())


execute_batch_validation(engine)

end_time = datetime.now()
print("Historical Load ended at", end_time)
print("Total time in seconds:", (end_time - start_time).total_seconds())
