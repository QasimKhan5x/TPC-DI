import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')

from scripts.util import execute_batch_validation

# Database connection details
host = "localhost"
user = "root"
password = "password"
database = "tpcdi_sf5"

# Create the SQLAlchemy engine
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}/{database}?allow_local_infile=true"
)
engine2 = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}/{database}?allow_local_infile=true"
)

# Flag to indicate when to stop the visiblity queries thread
stop_query_thread1 = False
stop_query_thread2 = False

def run_periodic_queries():
    q1 = """insert into DImessages
    select
         CURRENT_TIMESTAMP() as MessageDateAndTime
        ,case when BatchID is null then 0 else BatchID end as BatchID
        ,MessageSource
        ,MessageText 
        ,'Visibility_1' as MessageType
        ,MessageData
    from (
        select max(BatchID) as BatchID from DImessages 
    ) x join (

        /* Basic row counts */
           select 'DimAccount' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimAccount
        union select 'DimBroker' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimBroker
        union select 'DimCompany' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimCompany
        union select 'DimCustomer' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimCustomer
        union select 'DimDate' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimDate
        union select 'DimSecurity' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimSecurity
        union select 'DimTime' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimTime
        union select 'DimTrade' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimTrade
        union select 'Financial' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from Financial
        union select 'Industry' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from Industry
        union select 'Prospect' as MessageSource, 'Row count' as MessageText,
                 count(*) as MessageData from Prospect
        union select 'StatusType' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from StatusType
        union select 'TaxRate' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from TaxRate
        union select 'TradeType' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from TradeType
        /* Row counts for Fact tables */
        union select 'FactCashBalances' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactCashBalances
        union select 'FactCashBalances' as MessageSource, 'Row count joined' as MessageText, 
                count(*) as MessageData 
                from FactCashBalances f
                inner join DimAccount a on f.SK_AccountID = a.SK_AccountID
                inner join DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
                inner join DimBroker b on a.SK_BrokerID = b.SK_BrokerID
                inner join DimDate d on f.SK_DateID = d.SK_DateID
        union select 'FactHoldings' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactHoldings
        union select 'FactHoldings' as MessageSource, 'Row count joined' as MessageText, 
                count(*) as MessageData 
                from FactHoldings f
                inner join DimAccount a on f.SK_AccountID = a.SK_AccountID
                inner join DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
                inner join DimBroker b on a.SK_BrokerID = b.SK_BrokerID
                inner join DimDate d on f.SK_DateID = d.SK_DateID
                inner join DimTime t on f.SK_TimeID = t.SK_TimeID
                inner join DimCompany m on f.SK_CompanyID = m.SK_CompanyID
                inner join DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
        union select 'FactMarketHistory' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactMarketHistory
        union select 'FactMarketHistory' as MessageSource, 'Row count joined' as MessageText, 
                count(*) as MessageData 
                from FactMarketHistory f
                inner join DimDate d on f.SK_DateID = d.SK_DateID
                inner join DimCompany m on f.SK_CompanyID = m.SK_CompanyID
                inner join DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
        union select 'FactWatches' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactWatches
        union select 'FactWatches' as MessageSource, 'Row count joined' as MessageText, 
                count(*) as MessageData 
                from FactWatches f
                inner join DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
                inner join DimDate dp on f.SK_DateID_DatePlaced = dp.SK_DateID
                inner join DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
    ) y on 1=1;"""
    q2 = """insert into DImessages
    select
         CURRENT_TIMESTAMP() as MessageDateAndTime
        ,case when BatchID is null then 0 else BatchID end as BatchID
        ,MessageSource
        ,MessageText 
        ,'Visibility_2' as MessageType
        ,MessageData
    from (
        select max(BatchID) as BatchID from DImessages 
    ) x join (

        /* Basic row counts */
           select 'DimAccount' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimAccount
        union select 'DimBroker' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimBroker
        union select 'DimCompany' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimCompany
        union select 'DimCustomer' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimCustomer
        union select 'DimDate' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimDate
        union select 'DimSecurity' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimSecurity
        union select 'DimTime' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimTime
        union select 'DimTrade' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from DimTrade
        union select 'Financial' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from Financial
        union select 'Industry' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from Industry
        union select 'Prospect' as MessageSource, 'Row count' as MessageText,
                 count(*) as MessageData from Prospect
        union select 'StatusType' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from StatusType
        union select 'TaxRate' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from TaxRate
        union select 'TradeType' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from TradeType
        /* Row counts for Fact tables */
        union select 'FactCashBalances' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactCashBalances
        union select 'FactHoldings' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactHoldings
        union select 'FactMarketHistory' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactMarketHistory
        union select 'FactWatches' as MessageSource, 'Row count' as MessageText, 
                count(*) as MessageData from FactWatches
    ) y on 1=1;"""
    
    # phase 2
    global stop_query_thread1
    while not stop_query_thread1:
        # small interval before starting
        time.sleep(1.5)
        with engine2.connect() as connection:
            # visibility 1
            connection.execute(text(q1))
            connection.commit()
            # interval bw q1 and q2
            time.sleep(1.5)
            # visibility 2
            connection.execute(text(q2))
            connection.commit()
        
    # phase 3
    global stop_query_thread2
    while not stop_query_thread2:
        # small interval before starting
        time.sleep(1.5)
        with engine2.connect() as connection:
            # visibility 1
            connection.execute(text(q1))
            connection.commit()
            # interval bw q1 and q2
            time.sleep(1.5)
            # visibility 2
            connection.execute(text(q2))
            connection.commit()

# ## Incremental Load


def incremental_load(BATCH_ID):
    DATA_DIR = f"data\\sf5\\Batch{BATCH_ID}\\"

    with open(DATA_DIR + "BatchDate.txt") as f:
        BATCH_DATE = f.read().strip()
    BATCH_DATE = pd.to_datetime(BATCH_DATE)

    # ### dimCustomer & Prospect

    # #### dimCustomer

    # Define column names
    column_names = [
        "CDC_FLAG",
        "CDC_DSN",
        "C_ID",
        "C_TAX_ID",
        "C_ST_ID",
        "C_L_NAME",
        "C_F_NAME",
        "C_M_NAME",
        "C_GNDR",
        "C_TIER",
        "C_DOB",
        "C_ADLINE1",
        "C_ADLINE2",
        "C_ZIPCODE",
        "C_CITY",
        "C_STATE_PROV",
        "C_CTRY",
        "C_CTRY_1",
        "C_AREA_1",
        "C_LOCAL_1",
        "C_EXT_1",
        "C_CTRY_2",
        "C_AREA_2",
        "C_LOCAL_2",
        "C_EXT_2",
        "C_CTRY_3",
        "C_AREA_3",
        "C_LOCAL_3",
        "C_EXT_3",
        "C_EMAIL_1",
        "C_EMAIL_2",
        "C_LCL_TX_ID",
        "C_NAT_TX_ID",
    ]

    # Define data types
    data_types = {
        "CDC_FLAG": "category",
        "CDC_DSN": "int64",
        "C_ID": "int64",
        "C_TAX_ID": "str",
        "C_ST_ID": "category",
        "C_L_NAME": "str",
        "C_F_NAME": "str",
        "C_M_NAME": "str",
        "C_GNDR": "category",
        "C_TIER": "int64",
        "C_DOB": "str",
        "C_ADLINE1": "str",
        "C_ADLINE2": "str",
        "C_ZIPCODE": "str",
        "C_CITY": "str",
        "C_STATE_PROV": "str",
        "C_CTRY": "str",
        "C_CTRY_1": "str",
        "C_AREA_1": "str",
        "C_LOCAL_1": "str",
        "C_EXT_1": "str",
        "C_CTRY_2": "str",
        "C_AREA_2": "str",
        "C_LOCAL_2": "str",
        "C_EXT_2": "str",
        "C_CTRY_3": "str",
        "C_AREA_3": "str",
        "C_LOCAL_3": "str",
        "C_EXT_3": "str",
        "C_EMAIL_1": "str",
        "C_EMAIL_2": "str",
        "C_LCL_TX_ID": "str",
        "C_NAT_TX_ID": "str",
    }

    # Read the file
    file_path = DATA_DIR + "Customer.txt"
    df = pd.read_csv(
        file_path,
        sep="|",
        header=None,
        names=column_names,
        dtype=data_types,
        parse_dates=["C_DOB"],
    )

    # Rename columns
    df.rename(
        columns={
            "C_ID": "CustomerID",
            "C_TAX_ID": "TaxID",
            "C_L_NAME": "LastName",
            "C_F_NAME": "FirstName",
            "C_M_NAME": "MiddleInitial",
            "C_TIER": "Tier",
            "C_DOB": "DOB",
            "C_EMAIL_1": "Email1",
            "C_EMAIL_2": "Email2",
        },
        inplace=True,
    )

    df["Gender"] = df["C_GNDR"].str.upper()
    df.loc[~df["Gender"].isin(["M", "F"]), "Gender"] = "U"

    df.rename(
        columns={
            "C_ADLINE1": "AddressLine1",
            "C_ADLINE2": "AddressLine2",
            "C_ZIPCODE": "PostalCode",
            "C_CITY": "City",
            "C_STATE_PROV": "StateProv",
            "C_CTRY": "Country",
        },
        inplace=True,
    )

    # Status is copied from ST_NAME of the StatusType table by matching C_ST_ID with ST_ID of the StatusType table.
    status_mapping = (
        pd.read_sql_query("SELECT ST_ID AS C_ST_ID, ST_NAME from StatusType", engine)
        .set_index("C_ST_ID")["ST_NAME"]
        .to_dict()
    )
    df.loc[:, "Status"] = df.loc[:, "C_ST_ID"].map(status_mapping)

    def format_phone_number(row, i):
        # Extract components of the phone number
        ctry_code = row[f"C_CTRY_{i}"]
        area_code = row[f"C_AREA_{i}"]
        local = row[f"C_LOCAL_{i}"]
        ext = row[f"C_EXT_{i}"]

        if pd.isna(ctry_code):
            ctry_code = None
        if pd.isna(area_code):
            area_code = None
        if pd.isna(local):
            local = None
        if pd.isna(ext):
            ext = None

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
            phone += f"{ext}"

        return phone

    for i in range(1, 4):
        df[f"Phone{i}"] = df.apply(format_phone_number, axis=1, args=(i,))

    tax_info = pd.read_sql_query(
        "SELECT TX_ID, TX_NAME, TX_RATE FROM TaxRate", engine
    ).set_index("TX_ID")
    tax_name_mapping = tax_info["TX_NAME"].to_dict()
    tax_rate_mapping = tax_info["TX_RATE"].to_dict()

    # NationalTaxRateDesc and NationalTaxRate are copied from TX_NAME and TX_RATE respectively by matching C_NAT_TX_ID with TX_ID.
    df.loc[:, "NationalTaxRateDesc"] = df.loc[:, "C_NAT_TX_ID"].map(tax_name_mapping)
    df.loc[:, "NationalTaxRate"] = df.loc[:, "C_NAT_TX_ID"].map(tax_rate_mapping)

    # LocalTaxRateDesc and LocalTaxRate are copied from TX_NAME and TX_RATE respectively by matching C_LCL_TX_ID with TX_ID.
    df.loc[:, "LocalTaxRateDesc"] = df.loc[:, "C_LCL_TX_ID"].map(tax_name_mapping)
    df.loc[:, "LocalTaxRate"] = df.loc[:, "C_LCL_TX_ID"].map(tax_rate_mapping)

    df.loc[:, "IsCurrent"] = 1
    df.loc[:, "EffectiveDate"] = pd.to_datetime(BATCH_DATE)
    df.loc[:, "EndDate"] = pd.Timestamp("9999-12-31")
    df.loc[:, "BatchID"] = BATCH_ID

    # #### Prospect

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
        raw_prospect_df = pd.read_csv(
            filepath, header=None, names=columns, dtype=dtypes
        )

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
    prospect_df = pd.DataFrame(
        {col: pd.Series(dtype=typ) for col, typ in dtypes.items()}
    )

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
        f"SELECT SK_DateID FROM DimDate where DateValue = '{BATCH_DATE}'", engine
    ).iloc[0, 0]

    # Load old data from MySQL
    old_prospect_df = pd.read_sql("SELECT * FROM Prospect", engine)

    # Merge new and old data on AgencyID
    merged_df = pd.merge(
        prospect_df,
        old_prospect_df,
        on="AgencyID",
        suffixes=("_new", "_old"),
        how="left",
    )

    # List of fields to compare
    fields_to_compare = [
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

    # Identify changed records
    is_changed = (
        merged_df[[f + "_new" for f in fields_to_compare]]
        != merged_df[[f + "_old" for f in fields_to_compare]].values
    )
    has_changed = is_changed.any(axis=1)
    # only update the records if the left join succeeded
    same_agency_id = ~merged_df["SK_UpdateDateID_old"].isna()

    # Update SK_UpdateDateID for changed records
    merged_df.loc[(has_changed) & (same_agency_id), "SK_UpdateDateID_new"] = sk_dateid
    # the old ones keep the old SK_UpdateDateID
    merged_df.loc[
        (~has_changed) & (same_agency_id), "SK_UpdateDateID_new"
    ] = merged_df.loc[(~has_changed) & (same_agency_id), "SK_UpdateDateID_old"]

    # For new records (those that are NaN in old data), set SK_UpdateDateID to batch_date_sk
    is_new_record = merged_df["SK_UpdateDateID_old"].isna()
    merged_df.loc[is_new_record, "SK_UpdateDateID_new"] = sk_dateid

    # Finalize the DataFrame with updated SK_UpdateDateID
    prospect_df_updated = merged_df[
        ["AgencyID"] + [f + "_new" for f in fields_to_compare] + ["SK_UpdateDateID_new"]
    ]

    # rename cols
    cols = prospect_df_updated.columns.tolist()
    cols = {col: col.replace("_new", "") for col in cols}
    prospect_df_updated.rename(columns=cols, inplace=1)

    # SK_RecordDateID is set to the DimDate SK_DateID field that corresponds to the Batch Date.
    prospect_df_updated["SK_RecordDateID"] = sk_dateid
    prospect_df_updated["SK_RecordDateID"] = prospect_df_updated[
        "SK_RecordDateID"
    ].astype("uint32")

    # Define conditions for each tag with null checks
    conditions = {
        "HighValue": (
            prospect_df_updated["NetWorth"].notnull()
            & prospect_df_updated["Income"].notnull()
        )
        & (
            (prospect_df_updated["NetWorth"] > 1_000_000)
            | (prospect_df_updated["Income"] > 200_000)
        ),
        "Expenses": (
            prospect_df_updated["NumberChildren"].notnull()
            & prospect_df_updated["NumberCreditCards"].notnull()
        )
        & (
            (prospect_df_updated["NumberChildren"] > 3)
            | (prospect_df_updated["NumberCreditCards"] > 5)
        ),
        "Boomer": prospect_df_updated["Age"].notnull()
        & (prospect_df_updated["Age"] > 45),
        "MoneyAlert": (
            prospect_df_updated["Income"].notnull()
            & prospect_df_updated["CreditRating"].notnull()
            & prospect_df_updated["NetWorth"].notnull()
        )
        & (
            (prospect_df_updated["Income"] < 50_000)
            | (prospect_df_updated["CreditRating"] < 600)
            | (prospect_df_updated["NetWorth"] < 100_000)
        ),
        "Spender": (
            prospect_df_updated["NumberCars"].notnull()
            & prospect_df_updated["NumberCreditCards"].notnull()
        )
        & (
            (prospect_df_updated["NumberCars"] > 3)
            | (prospect_df_updated["NumberCreditCards"] > 7)
        ),
        "Inherited": (
            prospect_df_updated["Age"].notnull()
            & prospect_df_updated["NetWorth"].notnull()
        )
        & (
            (prospect_df_updated["Age"] < 25)
            & (prospect_df_updated["NetWorth"] > 1_000_000)
        ),
    }

    # Apply conditions to assign tags
    prospect_df_updated["MarketingNameplate"] = ""
    for tag, condition in conditions.items():
        prospect_df_updated["MarketingNameplate"] += np.where(condition, tag + "+", "")

    # Remove trailing '+' and replace empty strings with None
    prospect_df_updated["MarketingNameplate"] = (
        prospect_df_updated["MarketingNameplate"].str.rstrip("+").replace("", None)
    )

    prospect_df_updated["BatchID"] = BATCH_ID

    # #### Update Both

    # temporary prospect_df for matching
    prospect_df_temp = prospect_df_updated[
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

    for index, row in df.iterrows():
        # first check previous batch
        first_name, last_name = row["FirstName"].upper(), row["LastName"].upper()
        address1, address2 = row["AddressLine1"], row["AddressLine2"]
        postcode = row["PostalCode"]
        if not pd.isna(address1):
            address1 = address1.upper()
        if not pd.isna(address2):
            address2 = address1.upper()
        if not pd.isna(postcode):
            postcode = postcode.upper()

        query = f"""SELECT AgencyID, CreditRating, NetWorth, MarketingNameplate FROM Prospect
        WHERE UPPER(FirstName) = '{first_name}' AND UPPER(LastName) = '{last_name}'"""
        if pd.isna(row["AddressLine1"]):
            query += " AND AddressLine1 IS NULL"
        else:
            query += f" AND UPPER(AddressLine1) = '{address1}'"
        if pd.isna(row["AddressLine2"]):
            query += " AND AddressLine2 IS NULL"
        else:
            query += f" AND UPPER(AddressLine2) = '{address2}'"
        if pd.isna(row["PostalCode"]):
            query += " AND PostalCode IS NULL;"
        else:
            query += f" AND UPPER(PostalCode) = '{postcode}';"

        result = pd.read_sql_query(query, engine)
        if len(result) > 0:
            df.loc[index, "AgencyID"] = result.iloc[0, 0]
            df.loc[index, "CreditRating"] = result.iloc[0, 1]
            df.loc[index, "NetWorth"] = result.iloc[0, 2]
            df.loc[index, "MarketingNameplate"] = result.iloc[0, 3]
        else:
            # check current batch
            match = prospect_df_temp[
                (prospect_df_temp["LastName"] == last_name)
                & (prospect_df_temp["FirstName"] == first_name)
                & (prospect_df_temp["AddressLine1"] == address1)
                & (prospect_df_temp["AddressLine2"] == address2)
                & (prospect_df_temp["PostalCode"] == postcode)
            ]
            if not match.empty:
                df.loc[index, "AgencyID"] = match["AgencyID"].iloc[-1]
                df.loc[index, "CreditRating"] = match["CreditRating"].iloc[-1]
                df.loc[index, "NetWorth"] = match["NetWorth"].iloc[-1]
                df.loc[index, "MarketingNameplate"] = match["MarketingNameplate"].iloc[
                    -1
                ]
            else:
                # set empty
                df.loc[index, "AgencyID"] = None
                df.loc[index, "CreditRating"] = None
                df.loc[index, "NetWorth"] = None
                df.loc[index, "MarketingNameplate"] = None

    max_sk = pd.read_sql_query(
        "SELECT MAX(SK_CustomerID) FROM dimCustomer", engine
    ).iloc[0, 0]
    df.loc[:, "SK_CustomerID"] = range(max_sk + 1, max_sk + 1 + len(df))

    # A record will be reported in the DImessages table if a customer’s Tier is not one of the valid
    # values (1,2,3). The MessageSource is “DimCustomer”, the MessageType is “Alert” and the
    # MessageText is “Invalid customer tier”. The MessageData field is “C_ID = ” followed by the
    # key value of the record, then “, C_TIER = ” and the C_TIER value.

    invalid_tiers = df[~df["Tier"].isin([1, 2, 3])][["CustomerID", "Tier"]]
    if len(invalid_tiers) > 0:
        invalid_tiers.loc[:, "MessageDateAndTime"] = datetime.now()
        invalid_tiers["BatchID"] = BATCH_ID
        invalid_tiers["MessageSource"] = "DimCustomer"
        invalid_tiers["MessageText"] = "Invalid customer tier"
        invalid_tiers["MessageType"] = "Alert"
        invalid_tiers["MessageData"] = (
            "C_ID = "
            + invalid_tiers["CustomerID"].astype(str)
            + ", C_TIER = "
            + invalid_tiers["Tier"].astype(str)
        )

        sql_dtypes = {
            "MessageDateAndTime": sqlalchemy.types.DATETIME,
            "BatchID": sqlalchemy.types.Integer,
            "MessageSource": sqlalchemy.types.CHAR(30),
            "MessageText": sqlalchemy.types.CHAR(50),
            "MessageType": sqlalchemy.types.CHAR(12),
            "MessageData": sqlalchemy.types.CHAR(100),
        }
        invalid_tiers.to_sql(
            "dimessages", con=engine, if_exists="append", index=False, dtype=sql_dtypes
        )

    # A record will be reported in the DImessages table if a customer’s DOB is invalid. A customer’s
    # DOB is invalid if DOB < Batch Date – 100 years or DOB > Batch Date (customer over 100 years
    # old or born in the future). The MessageSource is “DimCustomer”, the MessageType is “Alert”
    # and the MessageText is “DOB out of range”. The MessageData field is “C_ID = ” followed by
    # the key value of the record, then “, C_DOB = ” and the C_DOB value.

    invalid_dobs = df[
        (df["DOB"] < BATCH_DATE - pd.Timedelta(days=100 * 365))
        | (df["DOB"] > BATCH_DATE + pd.Timedelta(days=100 * 365))
    ]
    if len(invalid_dobs) > 0:
        invalid_dobs.loc[:, "MessageDateAndTime"] = datetime.now()
        invalid_dobs["BatchID"] = BATCH_ID
        invalid_dobs["MessageSource"] = "DimCustomer"
        invalid_dobs["MessageText"] = "DOB out of range"
        invalid_dobs["MessageType"] = "Alert"
        invalid_dobs["MessageData"] = (
            "C_ID = "
            + invalid_tiers["CustomerID"].astype(str)
            + ", C_DOB = "
            + invalid_tiers["DOB"].astype(str)
        )

        sql_dtypes = {
            "MessageDateAndTime": sqlalchemy.types.DATETIME,
            "BatchID": sqlalchemy.types.Integer,
            "MessageSource": sqlalchemy.types.CHAR(30),
            "MessageText": sqlalchemy.types.CHAR(50),
            "MessageType": sqlalchemy.types.CHAR(12),
            "MessageData": sqlalchemy.types.CHAR(100),
        }
        invalid_dobs.to_sql(
            "dimessages", con=engine, if_exists="append", index=False, dtype=sql_dtypes
        )

    insert_customers_mask = df["CDC_FLAG"] == "I"
    keep_cols = [
        "SK_CustomerID",
        "CustomerID",
        "TaxID",
        "Status",
        "LastName",
        "FirstName",
        "MiddleInitial",
        "Gender",
        "Tier",
        "DOB",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
        "City",
        "StateProv",
        "Country",
        "Phone1",
        "Phone2",
        "Phone3",
        "Email1",
        "Email2",
        "NationalTaxRateDesc",
        "NationalTaxRate",
        "LocalTaxRateDesc",
        "LocalTaxRate",
        "AgencyID",
        "CreditRating",
        "NetWorth",
        "MarketingNameplate",
        "IsCurrent",
        "BatchID",
        "EffectiveDate",
        "EndDate",
    ]
    df_insert = df.loc[insert_customers_mask, keep_cols]

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
    df_insert.to_sql(
        "dimcustomer", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    update_customers_mask = df["CDC_FLAG"] == "U"
    keep_cols = [
        "SK_CustomerID",
        "CustomerID",
        "TaxID",
        "Status",
        "LastName",
        "FirstName",
        "MiddleInitial",
        "Gender",
        "Tier",
        "DOB",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
        "City",
        "StateProv",
        "Country",
        "Phone1",
        "Phone2",
        "Phone3",
        "Email1",
        "Email2",
        "NationalTaxRateDesc",
        "NationalTaxRate",
        "LocalTaxRateDesc",
        "LocalTaxRate",
        "AgencyID",
        "CreditRating",
        "NetWorth",
        "MarketingNameplate",
        "IsCurrent",
        "BatchID",
        "EffectiveDate",
        "EndDate",
    ]
    df_update = df.loc[update_customers_mask, keep_cols]

    df_update

    for index, row in tqdm(df_update.iterrows(), total=df_update.shape[0]):
        customer_id = row["CustomerID"]
        required_rows = df_update[df_update["CustomerID"].isin([customer_id])]
        # only one update per day
        if len(required_rows) > 1 and index != required_rows.index[-1]:
            continue
        effective_date = row["EffectiveDate"]

        # Obtain the current SK_CustomerID from dimCustomer
        current_customer = pd.read_sql_query(
            f"SELECT SK_CustomerID, Status FROM dimCustomer WHERE IsCurrent = 1 AND CustomerID = {customer_id}",
            engine,
        )

        sk_customer_id = current_customer["SK_CustomerID"].iloc[0]
        customer_status = current_customer["Status"].iloc[0]

        # Find all current records in dimAccount with the obtained SK_CustomerID
        current_accounts = pd.read_sql_query(
            f"SELECT * FROM dimAccount WHERE IsCurrent = 1 AND SK_CustomerID = {sk_customer_id}",
            engine,
        )

        # Update these records by setting IsCurrent to 0 and EndDate to effective_date
        update_query = f"""
        UPDATE dimAccount
        SET IsCurrent = 0, EndDate = '{effective_date}'
        WHERE IsCurrent = 1 AND SK_CustomerID = {sk_customer_id};
        """
        with engine.connect() as connection:
            connection.execute(text(update_query))
            connection.commit()
        # Prepare new account records
        new_accounts = current_accounts.copy()
        new_accounts["EffectiveDate"] = effective_date
        new_accounts["EndDate"] = pd.Timestamp("9999-12-31")
        new_accounts["Status"] = new_accounts["Status"].apply(
            lambda x: "INACTIVE" if customer_status == "INACTIVE" else x
        )
        new_accounts["IsCurrent"] = 1
        new_accounts["BatchID"] = BATCH_ID
        max_sk = pd.read_sql("SELECT MAX(SK_AccountID) FROM dimAccount", engine).iloc[
            0, 0
        ]
        new_accounts["SK_AccountID"] = range(
            max_sk + 1, max_sk + 1 + new_accounts.shape[0]
        )

        # Bulk insert new account records
        new_accounts.to_sql(
            "dimaccount",
            con=engine,
            if_exists="append",
            index=False,
            dtype={
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
            },
        )

        # History-tracking update for the customer
        history_update_query = f"""
        UPDATE dimCustomer
        SET IsCurrent = 0, EndDate = '{effective_date}'
        WHERE IsCurrent = 1 AND CustomerID = {customer_id};
        """
        with engine.connect() as connection:
            connection.execute(text(history_update_query))
            connection.commit()

    # Convert DataFrame columns to the appropriate types
    df_update["SK_CustomerID"] = df_update["SK_CustomerID"].astype(np.int32)
    df_update["CustomerID"] = df_update["CustomerID"].astype(np.int32)
    df_update["Tier"] = df_update["Tier"].astype(pd.Int8Dtype())
    df_update["NationalTaxRate"] = df_update["NationalTaxRate"].astype(np.float64)
    df_update["LocalTaxRate"] = df_update["LocalTaxRate"].astype(np.float64)
    df_update["CreditRating"] = df_update["CreditRating"].astype(pd.Int16Dtype())
    df_update["NetWorth"] = df_update["NetWorth"].astype(pd.Float64Dtype())
    df_update["BatchID"] = df_update["BatchID"].astype(np.int16)

    # Convert date columns to datetime.date
    df_update["DOB"] = pd.to_datetime(df_update["DOB"]).dt.date
    df_update["EffectiveDate"] = pd.to_datetime(df_update["EffectiveDate"]).dt.date
    df_update["EndDate"] = pd.to_datetime(df_update["EndDate"]).dt.date

    df_update.to_sql(
        "dimcustomer", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    active_customers = pd.read_sql_query(
        """SELECT UPPER(FirstName) FirstName , UPPER(LastName) LastName, UPPER(AddressLine1) AddressLine1,
    UPPER(AddressLine2) AddressLine2, UPPER(PostalCode) PostalCode FROM dimCustomer WHERE IsCurrent = 1 AND Status = 'ACTIVE';""",
        engine,
    )

    # Create temporary uppercase columns for merging in both DataFrames
    merge_fields = [
        "FirstName",
        "LastName",
        "AddressLine1",
        "AddressLine2",
        "PostalCode",
    ]
    merged_df = prospect_df_temp.merge(
        active_customers, how="left", on=merge_fields, indicator=True
    )

    mask = merged_df["_merge"] == "both"
    prospect_df_updated.loc[:, "IsCustomer"] = mask

    prospect_df_updated["SK_UpdateDateID"] = prospect_df_updated[
        "SK_UpdateDateID"
    ].astype("uint32")

    agency_ids = pd.read_sql_query(
        "SELECT DISTINCT AgencyID FROM Prospect", engine
    ).iloc[:, 0]

    existing_records = prospect_df_updated[
        prospect_df_updated["AgencyID"].isin(agency_ids)
    ]
    new_records = prospect_df_updated[~prospect_df_updated["AgencyID"].isin(agency_ids)]

    # Load data from the MySQL table 'prospect'
    prospect_df = pd.read_sql_table("prospect", con=engine)

    # Merge the DataFrame with the MySQL table data on 'AgencyID'
    merged_df = pd.merge(
        existing_records,
        prospect_df,
        on="AgencyID",
        suffixes=("_existing", "_prospect"),
    )

    # Define a function to compare rows, treating NaNs as equal
    def compare_rows(row, columns):
        for col in columns:
            # Both values are NaN
            if pd.isna(row[col + "_existing"]) and pd.isna(row[col + "_prospect"]):
                continue
            # One value is NaN and the other is not
            elif pd.isna(row[col + "_existing"]) or pd.isna(row[col + "_prospect"]):
                return False
            # Both values are not NaN, but are different
            elif row[col + "_existing"] != row[col + "_prospect"]:
                return False
        return True

    # Filter to find records where all columns are the same except 'IsCustomer'
    filter_columns = [
        col.replace("_existing", "")
        for col in merged_df.columns
        if "IsCustomer" not in col and "_existing" in col
    ]
    diff_is_customer = merged_df[
        merged_df.apply(
            lambda row: compare_rows(row, filter_columns)
            and (
                pd.isna(row["IsCustomer_existing"])
                != pd.isna(row["IsCustomer_prospect"])
                or row["IsCustomer_existing"] != row["IsCustomer_prospect"]
            ),
            axis=1,
        )
    ]

    # Count these records
    count_diff_is_customer = diff_is_customer.shape[0]

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

    new_records.to_sql(
        "prospect", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    with engine.connect() as connection:
        for _, row in tqdm(
            existing_records.iterrows(), total=existing_records.shape[0]
        ):
            agencyid = row["AgencyID"]
            query_parts = []
            for key, value in row.to_dict().items():
                if key != "AgencyID":
                    if pd.isna(value):
                        query_part = f"{key} = NULL"
                    elif isinstance(value, str):
                        value = value.replace('"', '\\"')
                        query_part = f'{key} = "{value}"'
                    else:  # for numeric and boolean types
                        query_part = f"{key} = {value}"
                    query_parts.append(query_part)
            query = f"UPDATE prospect SET {', '.join(query_parts)} WHERE AgencyID = \"{agencyid}\""
            connection.execute(text(query))
        connection.commit()

    num_rows = prospect_df_updated.shape[0]
    message_type = "Status"
    message_source = "Prospect"
    message_text = "Source rows"
    MessageDateAndTime = pd.Timestamp("now")

    query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
                VALUES ('{MessageDateAndTime}', {BATCH_ID}, '{message_source}', '{message_text}', '{message_type}', '{num_rows}')"""
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

    num_rows = new_records.shape[0]
    message_type = "Status"
    message_source = "Prospect"
    message_text = "Inserted rows"
    MessageDateAndTime = pd.Timestamp("now")

    query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
                VALUES ('{MessageDateAndTime}', {BATCH_ID}, '{message_source}', '{message_text}', '{message_type}', '{num_rows}')"""
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

    updated_rows = existing_records.shape[0] - count_diff_is_customer
    message_type = "Status"
    message_source = "Prospect"
    message_text = "Updated rows"
    MessageDateAndTime = pd.Timestamp("now")

    query = f"""INSERT INTO dimessages (MessageDateAndTime, BatchID, MessageSource, MessageText, MessageType, MessageData)
                VALUES ('{MessageDateAndTime}', {BATCH_ID}, '{message_source}', '{message_text}', '{message_type}', '{updated_rows}')"""
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

    # ### dimAccount

    df = pd.read_csv(
        DATA_DIR + "Account.txt",
        sep="|",
        header=None,
        names=[
            "CDC_FLAG",
            "CDC_DSN",
            "CA_ID",
            "CA_B_ID",
            "CA_C_ID",
            "CA_NAME",
            "CA_TAX_ST",
            "CA_ST_ID",
        ],
        dtype={
            "CDC_FLAG": "str",
            "CDC_DSN": "int64",
            "CA_ID": "int64",
            "CA_B_ID": "int64",
            "CA_C_ID": "int64",
            "CA_NAME": "str",
            "CA_TAX_ST": "uint8",
            "CA_ST_ID": "str",
        },
    )

    df.rename(
        {"CA_ID": "AccountID", "CA_NAME": "AccountDesc", "CA_TAX_ST": "TaxStatus"},
        axis=1,
        inplace=True,
    )

    broker_mapping = (
        pd.read_sql_query(
            "SELECT BrokerID AS CA_B_ID, SK_BrokerID FROM dimbroker WHERE isCurrent = 1",
            engine,
        )
        .set_index("CA_B_ID")["SK_BrokerID"]
        .to_dict()
    )
    customer_mapping = (
        pd.read_sql_query(
            "SELECT CustomerID AS CA_C_ID, SK_CustomerID FROM dimcustomer WHERE isCurrent = 1",
            engine,
        )
        .set_index("CA_C_ID")["SK_CustomerID"]
        .to_dict()
    )
    status_mapping = (
        pd.read_sql_query("SELECT ST_ID AS CA_ST_ID, ST_NAME FROM statustype", engine)
        .set_index("CA_ST_ID")["ST_NAME"]
        .to_dict()
    )

    df.loc[:, "SK_BrokerID"] = df["CA_B_ID"].map(broker_mapping)
    df.loc[:, "SK_CustomerID"] = df["CA_C_ID"].map(customer_mapping)
    df.loc[:, "Status"] = df["CA_ST_ID"].map(status_mapping)
    df.loc[:, "EffectiveDate"] = pd.to_datetime(BATCH_DATE)
    df.loc[:, "EndDate"] = pd.Timestamp("9999-12-31")
    df.loc[:, "BatchID"] = BATCH_ID
    df.loc[:, "IsCurrent"] = 1

    max_sk = pd.read_sql_query("SELECT MAX(SK_AccountID) FROM dimAccount", engine).iloc[
        0, 0
    ]
    df.loc[:, "SK_AccountID"] = range(max_sk + 1, max_sk + 1 + df.shape[0])

    drop_cols = ["CDC_FLAG", "CDC_DSN", "CA_B_ID", "CA_C_ID", "CA_ST_ID"]
    df_insert = df[df["CDC_FLAG"] == "I"].drop(columns=drop_cols)

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

    df_insert.to_sql(
        "dimaccount", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    drop_cols = ["CDC_FLAG", "CDC_DSN", "CA_B_ID", "CA_C_ID", "CA_ST_ID"]
    df_update = df[df["CDC_FLAG"] == "U"].drop(columns=drop_cols)

    with engine.connect() as connection:
        for index, row in df_update.iterrows():
            account_id = row["AccountID"]
            required_rows = df_update[df_update["AccountID"].isin([account_id])]
            # only one update per day
            if len(required_rows) > 1 and index != required_rows.index[-1]:
                continue
            effective_date = row["EffectiveDate"]
            query = f"""UPDATE dimAccount
            SET IsCurrent = 0, EndDate = '{effective_date}'
            WHERE IsCurrent = 1 AND AccountID = {account_id};"""
            connection.execute(text(query))
        connection.commit()

    df_update.to_sql(
        "dimaccount", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    # ### dimTrade

    column_names = [
        "CDC_FLAG",
        "CDC_DSN",
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

    data_types = {
        "CDC_FLAG": "category",
        "CDC_DSN": "int64",
        "T_ID": "int64",
        "T_DTS": "str",
        "T_ST_ID": "str",
        "T_TT_ID": "str",
        "T_IS_CASH": "boolean",
        "T_S_SYMB": "str",
        "T_QTY": "int64",
        "T_BID_PRICE": "float64",
        "T_CA_ID": "int64",
        "T_EXEC_NAME": "str",
        "T_TRADE_PRICE": "float64",
        "T_CHRG": "float64",
        "T_COMM": "float64",
        "T_TAX": "float64",
    }

    df = pd.read_csv(
        DATA_DIR + "Trade.txt",
        sep="|",
        header=None,
        names=column_names,
        dtype=data_types,
        parse_dates=["T_DTS"],
    )

    date_mapping = (
        pd.read_sql_query("SELECT SK_DateID, DateValue FROM DimDate", engine)
        .set_index("DateValue")["SK_DateID"]
        .to_dict()
    )
    time_mapping = pd.read_sql_query("SELECT SK_TimeID, TimeValue FROM DimTime", engine)

    def timedelta_to_time(td):
        return (datetime.min + td).time()

    # Applying the function to the series
    time_mapping["TimeValue"] = time_mapping["TimeValue"].apply(timedelta_to_time)
    time_mapping = time_mapping.set_index("TimeValue")["SK_TimeID"].to_dict()

    # If this is a new Trade record (CDC_FLAG = “I”) then SK_CreateDateID and
    # SK_CreateTimeID must be set based on T_DTS. SK_CloseDateID and SK_CloseTimeID must be set to NULL
    insert_mask = df["CDC_FLAG"] == "I"
    df.loc[insert_mask, "SK_CreateDateID"] = df.loc[insert_mask, "T_DTS"].dt.date.map(
        date_mapping
    )
    df.loc[insert_mask, "SK_CreateTimeID"] = df.loc[insert_mask, "T_DTS"].dt.time.map(
        time_mapping
    )
    df.loc[insert_mask, "SK_CloseDateID"] = None
    df.loc[insert_mask, "SK_CloseTimeID"] = None

    # If T_ST_ID is “CMPT” or “CNCL”, SK_CloseDateID and SK_CloseTimeID must be set based on T_DTS.
    close_mask = df["T_ST_ID"].isin(["CMPT", "CNCL"])
    df.loc[close_mask, "SK_CloseDateID"] = df.loc[close_mask, "T_DTS"].dt.date.map(
        date_mapping
    )
    df.loc[close_mask, "SK_CloseTimeID"] = df.loc[close_mask, "T_DTS"].dt.time.map(
        time_mapping
    )

    df.rename(
        columns={
            "T_ID": "TradeID",
            "T_IS_CASH": "CashFlag",
            "T_QTY": "Quantity",
            "T_BID_PRICE": "BidPrice",
            "T_EXEC_NAME": "ExecutedBy",
            "T_TRADE_PRICE": "TradePrice",
            "T_CHRG": "Fee",
            "T_COMM": "Commission",
            "T_TAX": "Tax",
        },
        inplace=True,
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
    df["Status"] = df["T_ST_ID"].map(status_mapping)
    df["Type"] = df["T_TT_ID"].map(trade_type_mapping)

    # Fetching security and account info in one go
    security_info = pd.read_sql(
        "SELECT Symbol, SK_SecurityID, SK_CompanyID FROM dimsecurity WHERE IsCurrent = 1",
        engine,
    ).set_index("Symbol")
    account_info = pd.read_sql(
        "SELECT AccountID, SK_AccountID, SK_CustomerID, SK_BrokerID FROM dimaccount WHERE IsCurrent = 1",
        engine,
    ).set_index("AccountID")

    df.loc[:, "SK_SecurityID"] = df.loc[:, "T_S_SYMB"].map(
        security_info["SK_SecurityID"].to_dict()
    )
    df.loc[:, "SK_CompanyID"] = df.loc[:, "T_S_SYMB"].map(
        security_info["SK_CompanyID"].to_dict()
    )
    df.loc[:, "SK_AccountID"] = df.loc[:, "T_CA_ID"].map(
        account_info["SK_AccountID"].to_dict()
    )
    df.loc[:, "SK_CustomerID"] = df.loc[:, "T_CA_ID"].map(
        account_info["SK_CustomerID"].to_dict()
    )
    df.loc[:, "SK_BrokerID"] = df.loc[:, "T_CA_ID"].map(
        account_info["SK_BrokerID"].to_dict()
    )

    df["BatchID"] = BATCH_ID

    # Filter the DataFrame
    invalid_trades = df[
        (df["Commission"].notnull())
        & (df["Commission"] > (df["TradePrice"] * df["Quantity"]))
    ]

    if invalid_trades.shape[0] > 0:
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
            query += f"""('{pd.Timestamp("now")}', {BATCH_ID}, '{MessageSource[i]}', '{MessageText[i]}', '{MessageType[i]}', '{MessageData[i]}'),"""
        with engine.connect() as conn:
            conn.execute(text(query[:-1]))
            conn.commit()

    # Filter the DataFrame for invalid trade fees
    invalid_fee_trades = df[
        (df["Fee"].notnull()) & (df["Fee"] > (df["TradePrice"] / df["Quantity"]))
    ]

    if len(invalid_fee_trades) > 0:
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
            query += f"""('{pd.Timestamp("now")}', {BATCH_ID}, '{MessageSource[i]}', '{MessageText[i]}', '{MessageType[i]}', '{MessageData[i]}'),"""
        with engine.connect() as conn:
            conn.execute(text(query[:-1]))
            conn.commit()

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
    keep_cols = [col for col in df.columns if col in sql_dtypes]

    df_insert = df.loc[df["CDC_FLAG"] == "I", keep_cols]

    df_insert.to_sql(
        "dimtrade", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    df_update = df.loc[df["CDC_FLAG"] == "U", keep_cols]

    with engine.connect() as connection:
        for index, row in df_update.iterrows():
            trade_id = row["TradeID"]
            required_rows = df_update[df_update["TradeID"] == trade_id]
            # only update using latest value
            if len(required_rows) > 1 and index != required_rows.index[-1]:
                continue
            str_cols = ["Status", "Type"]
            query_parts = []
            for key, value in row.to_dict().items():
                if key in ("TradeID", "SK_CreateDateID", "SK_CreateTimeID"):
                    continue
                if pd.isna(value):
                    query_part = f"{key} = NULL"
                elif key in str_cols:
                    value = value.replace("'", "\\'")
                    query_part = f"{key} = '{value}'"
                else:  # for numeric and boolean types
                    query_part = f"{key} = {value}"
                query_parts.append(query_part)
            query = f"UPDATE dimtrade SET {', '.join(query_parts)} WHERE TradeID = {trade_id};"
            connection.execute(text(query))
            connection.commit()

    # ### FactCashBalances

    df = pd.read_csv(
        DATA_DIR + "CashTransaction.txt",
        sep="|",
        header=None,
        names=["CDC_FLAG", "CDC_DSN", "CT_CA_ID", "CT_DTS", "CT_AMT", "CT_NAME"],
        dtype={
            "CDC_FLAG": "category",
            "CDC_DSN": "int64",
            "CT_CA_ID": "uint32",
            "CT_DTS": "str",
            "CT_AMT": "float64",
            "CT_NAME": "str",
        },
        parse_dates=["CT_DTS"],
    )

    account_info = pd.read_sql(
        "SELECT AccountID, SK_AccountID, SK_CustomerID FROM dimaccount WHERE IsCurrent = 1",
        engine,
    ).set_index("AccountID")
    df.loc[:, "SK_AccountID"] = df.loc[:, "CT_CA_ID"].map(
        account_info["SK_AccountID"].to_dict()
    )
    df.loc[:, "SK_CustomerID"] = df.loc[:, "CT_CA_ID"].map(
        account_info["SK_CustomerID"].to_dict()
    )

    date_info = pd.read_sql("SELECT DateValue, SK_DateID FROM dimdate", engine)
    date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])
    df.loc[:, "SK_DateID"] = df.loc[:, "CT_DTS"].dt.date.map(
        date_info.set_index("DateValue")["SK_DateID"]
    )

    df.loc[:, "BatchID"] = BATCH_ID

    # Sort the DataFrame by account ID and transaction date
    df.sort_values(by=["CT_CA_ID", "CT_DTS"], inplace=True)

    # Create a new column to store the prior cash amount
    df["PriorCash"] = df.groupby("CT_CA_ID")["CT_AMT"].cumsum() - df["CT_AMT"]
    df["PriorCash"].fillna(0, inplace=True)

    # Calculate the cash balance
    df["Cash"] = df["PriorCash"] + df["CT_AMT"]
    df = df.groupby(["CT_CA_ID", "SK_DateID"]).last().reset_index()

    sql_dtypes = {
        "SK_CustomerID": sqlalchemy.types.Integer,
        "SK_AccountID": sqlalchemy.types.Integer,
        "SK_DateID": sqlalchemy.types.Integer,
        "Cash": sqlalchemy.types.DECIMAL(precision=15, scale=2),
        "BatchID": sqlalchemy.types.SmallInteger,
    }
    keep_cols = [col for col in df.columns if col in sql_dtypes]

    df_insert = df.loc[df["CDC_FLAG"] == "I", keep_cols]

    df_insert.to_sql(
        "factcashbalances", engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    # ### FactHoldings
    #

    # SQL queries
    sql_commands = [
        "DROP TABLE IF EXISTS TempHoldingHistory",
        """
        CREATE TEMPORARY TABLE TempHoldingHistory (
            CDC_FLAG CHAR(1) NOT NULL,
            CDC_DSN INT UNSIGNED NOT NULL,
            HH_H_T_ID INT UNSIGNED NOT NULL,
            HH_T_ID INT UNSIGNED NOT NULL,
            HH_BEFORE_QTY INT NOT NULL,
            HH_AFTER_QTY INT NOT NULL
        )
        """,
        f"""
        LOAD DATA LOCAL INFILE 'E:\\\\Documents\\\\BDMA\\\\ULB\\\\Data Warehouses\\\\tpc-di\\\\TPC-DI\\\\data\\\\sf5\\\\Batch{BATCH_ID}\\\\HoldingHistory.txt'
        INTO TABLE TempHoldingHistory
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\n'
        (CDC_FLAG, CDC_DSN, HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY)
        """,
        f"""
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
            {BATCH_ID} AS BatchID
        FROM 
            TempHoldingHistory thh
        JOIN 
            DimTrade dt ON thh.HH_T_ID = dt.TradeID
        """,
        "DROP TABLE TempHoldingHistory",
    ]

    # Executing the queries
    with engine.connect() as connection:
        for sql in sql_commands:
            connection.execute(text(sql))
            connection.commit()

    # ### FactWatches

    df = pd.read_csv(
        DATA_DIR + "WatchHistory.txt",
        sep="|",
        header=None,
        names=["CDC_FLAG", "CDC_DSN", "W_C_ID", "W_S_SYMB", "W_DTS", "W_ACTION"],
        dtype={
            "CDC_FLAG": "category",
            "CDC_DSN": "int64",
            "W_C_ID": "uint32",
            "W_S_SYMB": "str",
            "W_DTS": "str",
            "W_ACTION": "str",
        },
        parse_dates=["W_DTS"],
    )

    customer_mapping = (
        pd.read_sql_query(
            "SELECT CustomerID, SK_CustomerID FROM dimcustomer WHERE IsCurrent = 1",
            engine,
        )
        .set_index("CustomerID")["SK_CustomerID"]
        .to_dict()
    )

    security_mapping = (
        pd.read_sql_query(
            "SELECT Symbol, SK_SecurityID FROM dimsecurity WHERE IsCurrent = 1", engine
        )
        .set_index("Symbol")["SK_SecurityID"]
        .to_dict()
    )

    date_info = pd.read_sql_query("SELECT DateValue, SK_DateID FROM dimdate", engine)
    date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])
    date_mapping = date_info.set_index("DateValue")["SK_DateID"].to_dict()

    df.loc[:, "SK_CustomerID"] = df.loc[:, "W_C_ID"].map(customer_mapping)
    df.loc[:, "SK_SecurityID"] = df.loc[:, "W_S_SYMB"].map(security_mapping)
    df.loc[:, "SK_DateID_DatePlaced"] = df.loc[:, "W_DTS"].dt.date.map(date_mapping)
    df.loc[:, "BatchID"] = BATCH_ID

    # Mask for rows where W_ACTION is 'CNCL'
    mask_cncl = df["W_ACTION"] == "CNCL"
    df.loc[mask_cncl, "SK_DateID_DateRemoved"] = df.loc[mask_cncl, "W_DTS"].dt.date.map(
        date_mapping
    )

    df = df.groupby(["W_C_ID", "W_S_SYMB"]).first().reset_index()

    new_records_dict = {
        "SK_CustomerID": [],
        "SK_SecurityID": [],
        "SK_DateID_DatePlaced": [],
        "SK_DateID_DateRemoved": [],
        "BatchID": [],
    }

    def update_or_insert(row):
        with engine.connect() as connection:
            # Check if the record exists in factwatches
            existing = pd.read_sql_query(
                "SELECT * FROM factwatches WHERE SK_CustomerID = {} AND SK_SecurityID = {}".format(
                    row["SK_CustomerID"], row["SK_SecurityID"]
                ),
                connection,
            )

            # Handle NaN in SK_DateID_DateRemoved
            date_removed = (
                "NULL"
                if pd.isna(row["SK_DateID_DateRemoved"])
                else row["SK_DateID_DateRemoved"]
            )

            if not existing.empty:
                # Update the existing record
                query = """
                UPDATE factwatches
                SET SK_DateID_DatePlaced = {}, SK_DateID_DateRemoved = {}, BatchID = {}
                WHERE SK_CustomerID = {} AND SK_SecurityID = {}
                """.format(
                    row["SK_DateID_DatePlaced"],
                    date_removed,
                    row["BatchID"],
                    row["SK_CustomerID"],
                    row["SK_SecurityID"],
                )
                connection.execute(text(query))
                connection.commit()
            else:
                # Check for matching records in dimcustomer and dimsecurity
                customer_sk = pd.read_sql_query(
                    "SELECT SK_CustomerID FROM dimcustomer WHERE CustomerID = {}".format(
                        row["W_C_ID"]
                    ),
                    connection,
                )
                security_sk = pd.read_sql_query(
                    "SELECT SK_SecurityID FROM dimsecurity WHERE Symbol = '{}'".format(
                        row["W_S_SYMB"]
                    ),
                    connection,
                )

                # Check for each combination of SKs in factwatches
                for cust_id in customer_sk["SK_CustomerID"]:
                    for sec_id in security_sk["SK_SecurityID"]:
                        existing = pd.read_sql_query(
                            "SELECT * FROM factwatches WHERE SK_CustomerID = {} AND SK_SecurityID = {}".format(
                                cust_id, sec_id
                            ),
                            connection,
                        )
                        if not existing.empty:
                            # Update the existing record with new values
                            query = """
                            UPDATE factwatches
                            SET SK_DateID_DatePlaced = {}, SK_DateID_DateRemoved = {}, BatchID = {}
                            WHERE SK_CustomerID = {} AND SK_SecurityID = {}
                            """.format(
                                row["SK_DateID_DatePlaced"],
                                date_removed,
                                row["BatchID"],
                                cust_id,
                                sec_id,
                            )
                            connection.execute(text(query))
                            connection.commit()
                            return

                # Accumulate new record for bulk insert
                new_records_dict["SK_CustomerID"].append(row["SK_CustomerID"])
                new_records_dict["SK_SecurityID"].append(row["SK_SecurityID"])
                new_records_dict["SK_DateID_DatePlaced"].append(
                    row["SK_DateID_DatePlaced"]
                )
                new_records_dict["SK_DateID_DateRemoved"].append(
                    row["SK_DateID_DateRemoved"]
                    if pd.notna(row["SK_DateID_DateRemoved"])
                    else None
                )
                new_records_dict["BatchID"].append(row["BatchID"])

    # Iterate over each row in the DataFrame and accumulate new records
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        update_or_insert(row)

    sql_dtypes = {
        "SK_CustomerID": sqlalchemy.types.Integer,
        "SK_SecurityID": sqlalchemy.types.Integer,
        "SK_DateID_DatePlaced": sqlalchemy.types.Integer,
        "SK_DateID_DateRemoved": sqlalchemy.types.Integer,
        "BatchID": sqlalchemy.types.SmallInteger,
    }
    # Bulk insert new records
    new_records_df = pd.DataFrame(new_records_dict)
    new_records_df.to_sql(
        "factwatches", con=engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    # ### FactMarketHistory

    df = pd.read_csv(
        DATA_DIR + "DailyMarket.txt",
        sep="|",
        header=None,
        names=[
            "CDC_FLAG",
            "CDC_DSN",
            "DM_DATE",
            "DM_S_SYMB",
            "DM_CLOSE",
            "DM_HIGH",
            "DM_LOW",
            "DM_VOL",
        ],
        dtype={
            "CDC_FLAG": "category",
            "CDC_DSN": "int64",
            "DM_DATE": "str",
            "DM_S_SYMB": "str",
            "DM_CLOSE": "float32",
            "DM_HIGH": "float32",
            "DM_LOW": "float32",
            "DM_VOL": "int64",
        },
        parse_dates=["DM_DATE"],
    )

    # ClosePrice, DayHigh, DayLow, and Volume are copied from DM_CLOSE, DM_HIGH,
    # DM_LOW, and DM_VOL respectively.
    df["ClosePrice"] = df["DM_CLOSE"]
    df["DayHigh"] = df["DM_HIGH"]
    df["DayLow"] = df["DM_LOW"]
    df["Volume"] = df["DM_VOL"]

    security_info = pd.read_sql(
        "SELECT Symbol, SK_SecurityID, SK_CompanyID FROM dimsecurity WHERE IsCurrent = 1",
        engine,
    ).set_index("Symbol")
    security_mapping = security_info["SK_SecurityID"].to_dict()
    company_mapping = security_info["SK_CompanyID"].to_dict()

    df.loc[:, "SK_SecurityID"] = df.loc[:, "DM_S_SYMB"].map(security_mapping)
    df.loc[:, "SK_CompanyID"] = df.loc[:, "DM_S_SYMB"].map(company_mapping)

    date_info = pd.read_sql_query("SELECT DateValue, SK_DateID FROM dimdate", engine)
    date_info["DateValue"] = pd.to_datetime(date_info["DateValue"])
    date_mapping = date_info.set_index("DateValue")["SK_DateID"].to_dict()
    df.loc[:, "SK_DateID"] = df.loc[:, "DM_DATE"].dt.date.map(date_mapping)

    # Step 2: Sort the DataFrame
    df.sort_values(by="DM_DATE", inplace=True)

    # Step 3 & 4: Group by 'DM_S_SYMB' and apply rolling max
    rolling_max = df.groupby("DM_S_SYMB").rolling("365D", on="DM_DATE")["DM_HIGH"].max()

    # Reset index to make merging easier
    rolling_max = rolling_max.reset_index()

    # Step 5: Merge with the original DataFrame
    df = df.merge(
        rolling_max, on=["DM_S_SYMB", "DM_DATE"], suffixes=("", "_52WeekHigh")
    )

    # Rename the column for clarity
    df.rename(columns={"DM_HIGH_52WeekHigh": "FiftyTwoWeekHigh"}, inplace=True)

    rolling_rank = (
        df.groupby("DM_S_SYMB")
        .rolling("365D", on="DM_DATE")["DM_HIGH"]
        .rank(method="average", ascending=False)
        .reset_index()
        .rename(columns={"DM_HIGH": "Rank"})
    )
    rolling_rank["Rank"] = rolling_rank["Rank"].astype("uint32")
    # Apply the mask to select DM_DATE only for those rows, then forward fill
    mask = rolling_rank["Rank"] == 1
    rolling_rank["SK_FiftyTwoWeekHighDate"] = (
        rolling_rank["DM_DATE"].where(mask).ffill()
    )
    rolling_rank["SK_FiftyTwoWeekHighDate"] = rolling_rank[
        "SK_FiftyTwoWeekHighDate"
    ].dt.date.map(date_mapping)

    df = pd.concat([df, rolling_rank["SK_FiftyTwoWeekHighDate"]], axis=1)

    df.sort_values(by="DM_DATE", inplace=True)
    # Step 3 & 4: Group by 'DM_S_SYMB' and apply rolling min
    rolling_min = df.groupby("DM_S_SYMB").rolling("365D", on="DM_DATE")["DM_LOW"].min()
    # Reset index to make merging easier
    rolling_min = rolling_min.reset_index()
    # Step 5: Merge with the original DataFrame
    df = df.merge(rolling_min, on=["DM_S_SYMB", "DM_DATE"], suffixes=("", "_52WeekLow"))
    # Rename the column for clarity
    df.rename(columns={"DM_LOW_52WeekLow": "FiftyTwoWeekLow"}, inplace=True)

    rolling_rank = (
        df.groupby("DM_S_SYMB")
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
    ].dt.date.map(date_mapping)

    df = pd.concat([df, rolling_rank["SK_FiftyTwoWeekLowDate"]], axis=1)

    df["SK_SecurityID"] = df["SK_SecurityID"].astype("uint32")
    df["SK_CompanyID"] = df["SK_CompanyID"].astype("uint32")
    df["SK_DateID"] = df["SK_DateID"].astype("uint32")
    df["FiftyTwoWeekHigh"] = df["FiftyTwoWeekHigh"].astype("float32")
    df["SK_FiftyTwoWeekHighDate"] = df["SK_FiftyTwoWeekHighDate"].astype("uint32")
    df["FiftyTwoWeekLow"] = df["FiftyTwoWeekLow"].astype("float32")
    df["SK_FiftyTwoWeekLowDate"] = df["SK_FiftyTwoWeekLowDate"].astype("uint32")
    df["DM_S_SYMB"] = df["DM_S_SYMB"].astype("category")

    df.drop(columns=["DM_HIGH", "DM_LOW", "DM_VOL", "DM_CLOSE"], inplace=True)

    security_mapping = pd.read_sql(
        "SELECT Symbol, Dividend FROM dimsecurity WHERE IsCurrent = 1", engine
    ).set_index("Symbol")["Dividend"]
    df.loc[:, "Yield"] = (
        df.loc[:, "DM_S_SYMB"].map(security_mapping) / df.loc[:, "ClosePrice"] * 100
    )

    df["BatchID"] = BATCH_ID

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

    df.drop(columns=["CDC_FLAG", "CDC_DSN"], inplace=True)

    df.to_sql(
        "tempfactmarketprice",
        engine,
        if_exists="replace",
        index=False,
        dtype=sql_dtypes,
    )

    sql_commands = [
        "CREATE INDEX idx_sk_companyid ON tempfactmarketprice(SK_CompanyID);",
        """SELECT fmp.ClosePrice / T.Sum_EPS AS PERatio
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
        "DROP TABLE tempfactmarketprice;",
    ]

    with engine.connect() as connection:
        connection.execute(text(sql_commands[0]))
        connection.commit()
    peratio = pd.read_sql_query(sql_commands[1], engine)
    with engine.connect() as connection:
        connection.execute(text(sql_commands[2]))
        connection.commit()

    df = pd.concat([df, peratio], axis=1)

    no_earnings = pd.DataFrame(df.loc[df["PERatio"].isna(), "DM_S_SYMB"])
    no_earnings["MessageDateAndTime"] = datetime.now()
    no_earnings["BatchID"] = BATCH_ID
    no_earnings["MessageSource"] = "FactMarketHistory"
    no_earnings["MessageText"] = "No earnings for company"
    no_earnings["MessageType"] = "Alert"
    no_earnings.rename(columns={"DM_S_SYMB": "MessageData"}, inplace=True)
    no_earnings["MessageData"] = "DM_S_SYMB = " + no_earnings["MessageData"].astype(str)

    sql_dtypes = {
        "MessageDateAndTime": sqlalchemy.types.DATETIME,
        "BatchID": sqlalchemy.types.Integer,
        "MessageSource": sqlalchemy.types.CHAR(30),
        "MessageText": sqlalchemy.types.CHAR(50),
        "MessageType": sqlalchemy.types.CHAR(12),
        "MessageData": sqlalchemy.types.CHAR(100),
    }
    no_earnings.to_sql(
        "dimessages", con=engine, if_exists="append", index=False, dtype=sql_dtypes
    )

    df.drop(columns=["DM_DATE", "DM_S_SYMB"], inplace=True)

    sql_dtypes = {
        "SK_SecurityID": sqlalchemy.types.Integer,
        "SK_CompanyID": sqlalchemy.types.Integer,
        "SK_DateID": sqlalchemy.types.Integer,
        "PERatio": sqlalchemy.types.DECIMAL(precision=10, scale=2),
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
    }

    df.to_sql(
        "factmarkethistory",
        con=engine,
        if_exists="append",
        index=False,
        dtype=sql_dtypes,
    )


def run_incremental_phase():
    start_time = datetime.now()
    print("Starting incremental load 1 at {}".format(start_time))
    incremental_load(2)
    global stop_query_thread1
    stop_query_thread1 = True
    execute_batch_validation(engine)
    end_time = datetime.now()
    print(
        "Finished incremental load 1 at {}. Took {} seconds".format(
            end_time, (end_time - start_time).total_seconds()
        )
    )
    start_time = datetime.now()
    print("Starting incremental load 2 at {}".format(start_time))
    incremental_load(3)
    global stop_query_thread2
    stop_query_thread2 = True
    execute_batch_validation(engine)
    end_time = datetime.now()
    print(
        "Finished incremental load 2 at {}. Took {} seconds".format(
            end_time, (end_time - start_time).total_seconds()
        )
    )

if __name__ == "__main__":

    # Create threads for both tasks
    load_thread = threading.Thread(target=run_incremental_phase)
    # query_thread = threading.Thread(target=run_periodic_queries)
    
    # Start the incremental_load process
    load_thread.start()
    # Start visibility queries
    # query_thread.start()
    
    # Wait for the threads to finish
    load_thread.join()
    # query_thread.join()
    
    # drop the connection
    engine.dispose()
    engine2.dispose()
