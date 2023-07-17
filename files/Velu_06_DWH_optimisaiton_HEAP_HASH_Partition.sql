---optimisation query
/* 
CREATE TABLE [velu].[Sale_Heap]
(
    [TransactionId] [uniqueidentifier]  NOT NULL,
    [CustomerId] [int]  NOT NULL,
    [ProductId] [smallint]  NOT NULL,
    [Quantity] [smallint]  NOT NULL,
    [Price] [decimal](9,2)  NOT NULL,
    [TotalAmount] [decimal](9,2)  NOT NULL,
    [TransactionDateId] [int]  NOT NULL,
    [ProfitAmount] [decimal](9,2)  NOT NULL,
    [Hour] [tinyint]  NOT NULL,
    [Minute] [tinyint]  NOT NULL,
    [StoreId] [smallint]  NOT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
*/

SELECT
    MIN(AvgPrice) as MinCustomerAvgPrice
    ,MAX(AvgPrice) as MaxCustomerAvgPrice
    ,MIN(AvgTotalAmount) as MinCustomerAvgTotalAmount
    ,MAX(AvgTotalAmount) as MaxCustomerAvgTotalAmount
    ,MIN(AvgProfitAmount) as MinAvgProfitAmount
    ,MAX(AvgProfitAmount) as MaxAvgProfitAmount
FROM
(
    SELECT
        FS.CustomerID
        ,MIN(FS.Quantity) as MinQuantity
        ,MAX(FS.Quantity) as MaxQuantity
        ,AVG(FS.Price) as AvgPrice
        ,AVG(FS.TotalAmount) as AvgTotalAmount
        ,AVG(FS.ProfitAmount) as AvgProfitAmount
        ,COUNT(DISTINCT FS.StoreId) as DistinctStores
    FROM
        velu.Sale_Heap FS
    GROUP BY
        FS.CustomerId
) T

--- exlain
EXPLAIN WITH_RECOMMENDATIONS
SELECT TOP 1000 * FROM
(
    SELECT
        S.CustomerId
        ,SUM(S.TotalAmount) as TotalAmount
    FROM
        [velu].[Sale_Heap] S
    GROUP BY
        S.CustomerId
) T
-----
/*
<?xml version="1.0" encoding="utf-8"?>
<dsql_query number_nodes="1" number_distributions="60" number_distributions_per_node="60">
  <sql>SELECT TOP 1000 * FROM
(
    SELECT
        S.CustomerId
        ,SUM(S.TotalAmount) as TotalAmount
    FROM
        [velu].[Sale_Heap] S
    GROUP BY
        S.CustomerId
) T</sql>
  <materialized_view_candidates>
    <materialized_view_candidates with_constants="False">CREATE MATERIALIZED VIEW View1 WITH (DISTRIBUTION = HASH([Expr0])) AS
SELECT [S].[CustomerId] AS [Expr0],
       SUM([S].[TotalAmount]) AS [Expr1]
FROM [velu].[Sale_Heap] [S]
GROUP BY [S].[CustomerId]</materialized_view_candidates>
  </materialized_view_candidates>
  <dsql_operations total_cost="0.6588816" total_number_operations="5">
    <dsql_operation operation_type="RND_ID">
      <identifier>TEMP_ID_175</identifier>
    </dsql_operation>
    <dsql_operation operation_type="ON">
      <location permanent="false" distribution="AllDistributions" />
      <sql_operations>
        <sql_operation type="statement">CREATE TABLE [tempdb].[dbo].[TEMP_ID_175] ([CustomerId] INT NOT NULL, [col] DECIMAL(38, 2) NOT NULL ) WITH(DISTRIBUTED_MOVE_FILE='');</sql_operation>
      </sql_operations>
    </dsql_operation>
    <dsql_operation operation_type="SHUFFLE_MOVE">
      <operation_cost cost="0.6588816" accumulative_cost="0.6588816" average_rowsize="13" output_rows="12670.8" GroupNumber="11" />
      <source_statement>SELECT [T1_1].[CustomerId] AS [CustomerId], [T1_1].[col] AS [col] FROM (SELECT SUM([T2_1].[TotalAmount]) AS [col], [T2_1].[CustomerId] AS [CustomerId] FROM [SQLPool01].[velu].[Sale_Heap] AS T2_1 GROUP BY [T2_1].[CustomerId]) AS T1_1
OPTION (MAXDOP 1, MIN_GRANT_PERCENT = [MIN_GRANT], DISTRIBUTED_MOVE(N''))</source_statement>
      <destination_table>[TEMP_ID_175]</destination_table>
      <shuffle_columns>CustomerId;</shuffle_columns>
    </dsql_operation>
    <dsql_operation operation_type="RETURN">
      <location distribution="AllDistributions" />
      <select>SELECT [T1_1].[CustomerId] AS [CustomerId], [T1_1].[col] AS [col] FROM (SELECT TOP (CAST ((1000) AS BIGINT)) SUM([T2_1].[col]) AS [col], [T2_1].[CustomerId] AS [CustomerId] FROM [tempdb].[dbo].[TEMP_ID_175] AS T2_1 GROUP BY [T2_1].[CustomerId]) AS T1_1
OPTION (MAXDOP 1, MIN_GRANT_PERCENT = [MIN_GRANT])</select>
    </dsql_operation>
    <dsql_operation operation_type="ON">
      <location permanent="false" distribution="AllDistributions" />
      <sql_operations>
        <sql_operation type="statement">DROP TABLE [tempdb].[dbo].[TEMP_ID_175]</sql_operation>
      </sql_operations>
    </dsql_operation>
  </dsql_operations>
</dsql_query>
*/


--- create with HASH
CREATE TABLE [velu].[Sale_Hash]
WITH
(
    DISTRIBUTION = HASH ( [CustomerId] ),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT
    *
FROM
    [velu].[Sale_Heap];;
Go

--- check
SELECT TOP 1000 * FROM
(
    SELECT
        S.CustomerId
        ,SUM(S.TotalAmount) as TotalAmount
    FROM
        [velu].[Sale_Hash] S
    GROUP BY
        S.CustomerId
) T


---- explain

EXPLAIN
SELECT TOP 1000 * FROM
(
    SELECT
        S.CustomerId
        ,SUM(S.TotalAmount) as TotalAmount
    FROM
        [velu].[Sale_Hash] S
    GROUP BY
        S.CustomerId
) T

--- more complex for parition
SELECT
    AVG(TotalProfit) as AvgMonthlyCustomerProfit
FROM
(
    SELECT
        S.CustomerId
        ,D.Year
        ,D.Month
        ,SUM(S.TotalAmount) as TotalAmount
        ,AVG(S.TotalAmount) as AvgAmount
        ,SUM(S.ProfitAmount) as TotalProfit
        ,AVG(S.ProfitAmount) as AvgProfit
    FROM
        [velu].[Sale_Hash] S
        join [wwi].[Date] D on
            D.DateId = S.TransactionDateId
    GROUP BY
        S.CustomerId
        ,D.Year
        ,D.Month
) T
Go

--- paritioning
CREATE TABLE [velu].[Sale_Partition01]
WITH
(
	DISTRIBUTION = HASH ( [CustomerId] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[TransactionDateId] RANGE RIGHT FOR VALUES (
            20190101, 20190201, 20190301, 20190401, 20190501, 20190601, 20190701, 20190801, 20190901, 20191001, 20191101, 20191201)
	)
)
AS
SELECT
	*
FROM	
	[velu].[Sale_Heap]
OPTION  (LABEL  = 'CTAS : Sale_Partition01')
go


CREATE TABLE [velu].[Sale_Partition02]
WITH
(
	DISTRIBUTION = HASH ( [CustomerId] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[TransactionDateId] RANGE RIGHT FOR VALUES (
            20190101, 20190401, 20190701, 20191001)
	)
)
AS
SELECT *
FROM
    [velu].[Sale_Heap]
OPTION  (LABEL  = 'CTAS : Sale_Partition02')
GO


--- check
SELECT
    AVG(TotalProfit) as AvgMonthlyCustomerProfit
FROM
(
    SELECT
        S.CustomerId
        ,S.TransactionDateId
        ,SUM(S.TotalAmount) as TotalAmount
        ,AVG(S.TotalAmount) as AvgAmount
        ,SUM(S.ProfitAmount) as TotalProfit
        ,AVG(S.ProfitAmount) as AvgProfit
    FROM
        [velu].[Sale_Partition01] S
    GROUP BY
        S.CustomerId,
        S.TransactionDateId
) T
GO
---- create materialized view

CREATE MATERIALIZED VIEW
    velu.mvSaleCustomer
WITH
(
    DISTRIBUTION = HASH( CustomerId )
)
AS 
 SELECT
        S.CustomerId
        ,S.TransactionDateId
        ,SUM(S.TotalAmount) as TotalAmount
        ,AVG(S.TotalAmount) as AvgAmount
        ,SUM(S.ProfitAmount) as TotalProfit
        ,AVG(S.ProfitAmount) as AvgProfit
    FROM
        [velu].[Sale_Hash] S
    GROUP BY
        S.CustomerId,
        S.TransactionDateId
GO
 
---- explain
EXPLAIN 
select top 1100 * from ( 
 SELECT
        S.CustomerId
        ,S.TransactionDateId
        ,SUM(S.TotalAmount) as TotalAmount
        ,AVG(S.TotalAmount) as AvgAmount
        ,SUM(S.ProfitAmount) as TotalProfit
        ,AVG(S.ProfitAmount) as AvgProfit
    FROM
        [velu].[Sale_Hash] S
    GROUP BY
        S.CustomerId,
        S.TransactionDateId
) T