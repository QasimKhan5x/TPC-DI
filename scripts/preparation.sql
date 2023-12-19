CREATE TABLE DimAccount (
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
);
CREATE TABLE DimBroker (
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
);
CREATE TABLE DimCompany (
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
);
CREATE TABLE DimCustomer (
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
);
CREATE TABLE DimDate (
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
);
CREATE TABLE DimSecurity (
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
);
CREATE TABLE DimTime (
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
);
CREATE TABLE DimTrade (
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
);
CREATE TABLE DImessages (
    MessageDateAndTime DATETIME NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL,
    MessageSource CHAR(30),
    MessageText CHAR(50) NOT NULL,
    MessageType CHAR(12) NOT NULL,
    MessageData CHAR(100)
);
CREATE TABLE FactCashBalances (
    SK_CustomerID INT UNSIGNED NOT NULL,
    SK_AccountID INT UNSIGNED NOT NULL,
    SK_DateID INT UNSIGNED NOT NULL,
    Cash DECIMAL(15, 2) NOT NULL,
    BatchID SMALLINT UNSIGNED NOT NULL
);
CREATE TABLE FactHoldings (
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
);
CREATE TABLE FactMarketHistory (
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
);
CREATE TABLE FactWatches (
    SK_CustomerID INT UNSIGNED NOT NULL,
    SK_SecurityID INT UNSIGNED NOT NULL,
    SK_DateID_DatePlaced INT UNSIGNED NOT NULL,
    SK_DateID_DateRemoved INT UNSIGNED,
    BatchID SMALLINT UNSIGNED NOT NULL
);

CREATE TABLE Financial (
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
);
CREATE TABLE Prospect (
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
);
CREATE TABLE StatusType (
    ST_ID CHAR(4) NOT NULL,
    ST_NAME CHAR(10) NOT NULL,
    PRIMARY KEY (ST_ID)
);
CREATE TABLE TaxRate (
    TX_ID CHAR(4) NOT NULL,
    TX_NAME CHAR(50) NOT NULL,
    TX_RATE DECIMAL(6, 5) NOT NULL,
    PRIMARY KEY (TX_ID)
);
CREATE TABLE TradeType (
    TT_ID CHAR(3) NOT NULL,
    TT_NAME CHAR(12) NOT NULL,
    TT_IS_SELL TINYINT UNSIGNED NOT NULL CHECK (TT_IS_SELL IN (0, 1)),
    TT_IS_MRKT TINYINT UNSIGNED NOT NULL CHECK (TT_IS_MRKT IN (0, 1)),
    PRIMARY KEY (TT_ID)
);
CREATE TABLE Audit (
    DataSet CHAR(20) NOT NULL,
    BatchID SMALLINT UNSIGNED,
    Date DATE,
    Attribute CHAR(50) NOT NULL,
    Value BIGINT,
    DValue DECIMAL(15, 5),
    PRIMARY KEY (DataSet, Attribute)
);