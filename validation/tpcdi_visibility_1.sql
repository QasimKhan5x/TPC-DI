insert into DImessages
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
			
) y on 1=1
;
