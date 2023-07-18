/*
Create tables and procedures to generate the Date and Sales tables
*/
CREATE TABLE [velv2].[dimension_Date]
(
    [Date] [datetime] NOT NULL,
    [Day Number] [int] NOT NULL,
    [Day] [nvarchar](10) NOT NULL,
    [Month] [nvarchar](10) NOT NULL,
    [Short Month] [nvarchar](3) NOT NULL,
    [Calendar Month Number] [int] NOT NULL,
    [Calendar Month Label] [nvarchar](20) NOT NULL,
    [Calendar Year] [int] NOT NULL,
    [Calendar Year Label] [nvarchar](10) NOT NULL,
    [Fiscal Month Number] [int] NOT NULL,
    [Fiscal Month Label] [nvarchar](20) NOT NULL,
    [Fiscal Year] [int] NOT NULL,
    [Fiscal Year Label] [nvarchar](10) NOT NULL,
    [ISO Week Number] [int] NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX ([Date])
);
DROP TABLE  [velv2].[fact_Sale]
GO
CREATE TABLE [velv2].[fact_Sale]
(
    [Sale Key] [bigint] IDENTITY(1,1) NOT NULL,
    [City Key] [int] NOT NULL,
    [Customer Key] [int] NOT NULL,
    [Bill To Customer Key] [int] NOT NULL,
    [Stock Item Key] [int] NOT NULL,
    [Invoice Date Key] [date] NOT NULL,
    [Delivery Date Key] [date] NULL,
    [Salesperson Key] [int] NOT NULL,
    [WWI Invoice ID] [int] NOT NULL,
    [Description] [nvarchar](100) NOT NULL,
    [Package] [nvarchar](50) NOT NULL,
    [Quantity] [int] NOT NULL,
    [Unit Price] [decimal](18, 2) NOT NULL,
    [Tax Rate] [decimal](18, 3) NOT NULL,
    [Total Excluding Tax] [decimal](18, 2) NOT NULL,
    [Tax Amount] [decimal](18, 2) NOT NULL,
    [Profit] [decimal](18, 2) NOT NULL,
    [Total Including Tax] [decimal](18, 2) NOT NULL,
    [Total Dry Items] [int] NOT NULL,
    [Total Chiller Items] [int] NOT NULL,
    [Lineage Key] [int] NOT NULL
)
WITH
(
    DISTRIBUTION = HASH ( [WWI Invoice ID] ),
    CLUSTERED COLUMNSTORE INDEX
)
Go
drop PROCEDURE [velv2].[InitialSalesDataPopulation] 
go
CREATE PROCEDURE [velv2].[InitialSalesDataPopulation] AS
BEGIN
    INSERT INTO [velv2].[seed_Sale] (
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    )
    SELECT
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    FROM [velv2].[seed_Sale]
/*
    INSERT INTO [velv2].[seed_Sale] (
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    )
    SELECT
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    FROM [velv2].[seed_Sale]

    INSERT INTO [velv2].[seed_Sale] (
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    )
    SELECT
        [Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], [Profit], [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
    FROM [velv2].[seed_Sale]
    */
END
GO
DROP PROCEDURE [velv2].[PopulateDateDimensionForYear]
GO
CREATE PROCEDURE [velv2].[PopulateDateDimensionForYear] @Year [int] AS
BEGIN
    IF OBJECT_ID('tempdb..#month', 'U') IS NOT NULL
        DROP TABLE #month
    CREATE TABLE #month (
        monthnum int,
        numofdays int
    )
    WITH ( DISTRIBUTION = ROUND_ROBIN, heap )
    INSERT INTO #month
        SELECT 1, 31 UNION SELECT 2, CASE WHEN (@YEAR % 4 = 0 AND @YEAR % 100 <> 0) OR @YEAR % 400 = 0 THEN 29 ELSE 28 END UNION SELECT 3,31 UNION SELECT 4,30 UNION SELECT 5,31 UNION SELECT 6,30 UNION SELECT 7,31 UNION SELECT 8,31 UNION SELECT 9,30 UNION SELECT 10,31 UNION SELECT 11,30 UNION SELECT 12,31

    IF OBJECT_ID('tempdb..#days', 'U') IS NOT NULL
        DROP TABLE #days
    CREATE TABLE #days (days int)
    WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)

    INSERT INTO #days
        SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20    UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31

    INSERT [velv2].[dimension_Date] (
        [Date], [Day Number], [Day], [Month], [Short Month], [Calendar Month Number], [Calendar Month Label], [Calendar Year], [Calendar Year Label], [Fiscal Month Number], [Fiscal Month Label], [Fiscal Year], [Fiscal Year Label], [ISO Week Number]
    )
    SELECT
        CAST(CAST(monthnum AS VARCHAR(2)) + '/' + CAST([days] AS VARCHAR(3)) + '/' + CAST(@year AS CHAR(4)) AS DATE) AS [Date]
        ,DAY(CAST(CAST(monthnum AS VARCHAR(2)) + '/' + CAST([days] AS VARCHAR(3)) + '/' + CAST(@year AS CHAR(4)) AS DATE)) AS [Day Number]
        ,CAST(DATENAME(day, CAST(CAST(monthnum AS VARCHAR(2)) + '/' + CAST([days] AS VARCHAR(3)) + '/' + CAST(@year AS CHAR(4)) AS DATE)) AS NVARCHAR(10)) AS [Day]
        ,CAST(DATENAME(month, CAST(CAST(monthnum AS VARCHAR(2)) + '/' + CAST([days] AS VARCHAR(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS nvarchar(10)) AS [Month]
        ,CAST(SUBSTRING(DATENAME(month, CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)), 1, 3) AS nvarchar(3)) AS [Short Month]
        ,MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS [Calendar Month Number]
        ,CAST(N'CY' + CAST(YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS nvarchar(4)) + N'-' + SUBSTRING(DATENAME(month, CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)), 1, 3) AS nvarchar(10)) AS [Calendar Month Label]
        ,YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS [Calendar Year]
        ,CAST(N'CY' + CAST(YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS nvarchar(4)) AS nvarchar(10)) AS [Calendar Year Label]
        ,CASE WHEN MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) IN (11, 12)
        THEN MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) - 10
        ELSE MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) + 2 END AS [Fiscal Month Number]
        ,CAST(N'FY' + CAST(CASE WHEN MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) IN (11, 12)
        THEN YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) + 1
        ELSE YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) END AS nvarchar(4)) + N'-' + SUBSTRING(DATENAME(month, CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)), 1, 3) AS nvarchar(20)) AS [Fiscal Month Label]
        ,CASE WHEN MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) IN (11, 12)
        THEN YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) + 1
        ELSE YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) END AS [Fiscal Year]
        ,CAST(N'FY' + CAST(CASE WHEN MONTH(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) IN (11, 12)
        THEN YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) + 1
        ELSE YEAR(CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE))END AS nvarchar(4)) AS nvarchar(10)) AS [Fiscal Year Label]
        , DATEPART(ISO_WEEK, CAST(CAST(monthnum as varchar(2)) + '/' + CAST([days] as varchar(3)) + '/' + CAST(@year as char(4)) AS DATE)) AS [ISO Week Number]
FROM #month m
    CROSS JOIN #days d
WHERE d.days <= m.numofdays
DROP table #month;
DROP table #days;
END;
GO


----
DROP PROCEDURE [velv2].[Configuration_PopulateLargeSaleTable]
GO

CREATE PROCEDURE [velv2].[Configuration_PopulateLargeSaleTable] @EstimatedRowsPerDay [bigint],@Year [int] AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    EXEC [velv2].[PopulateDateDimensionForYear] @Year;

    DECLARE @OrderCounter bigint = 0;
    DECLARE @NumberOfSalesPerDay bigint = @EstimatedRowsPerDay;
    DECLARE @DateCounter date;
    DECLARE @StartingSaleKey bigint;
    DECLARE @MaximumSaleKey bigint = (SELECT MAX([Sale Key]) FROM velv2.seed_Sale);
    DECLARE @MaxDate date;
    SET @MaxDate = (SELECT MAX([Invoice Date Key]) FROM velv2.fact_Sale)
    IF ( @MaxDate < CAST(@YEAR AS CHAR(4)) + '1231') AND (@MaxDate > CAST(@YEAR AS CHAR(4)) + '0101')
        SET @DateCounter = @MaxDate
    ELSE
        SET @DateCounter= CAST(@Year as char(4)) + '0101';

    PRINT 'Targeting ' + CAST(@NumberOfSalesPerDay AS varchar(20)) + ' sales per day.';

    DECLARE @OutputCounter varchar(20);
    DECLARE @variance DECIMAL(18,10);
    DECLARE @VariantNumberOfSalesPerDay BIGINT;

    WHILE @DateCounter < CAST(@YEAR AS CHAR(4)) + '1231'
    BEGIN
        SET @OutputCounter = CONVERT(varchar(20), @DateCounter, 112);
        RAISERROR(@OutputCounter, 0, 1);
        SET @variance = (SELECT RAND() * 10)*.01 + .95
        SET @VariantNumberOfSalesPerDay = FLOOR(@NumberOfSalesPerDay * @variance)

        SET @StartingSaleKey = @MaximumSaleKey - @VariantNumberOfSalesPerDay - FLOOR(RAND() * 20000);
        SET @OrderCounter = 0;

        INSERT [velv2].[fact_Sale] (
            [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID], [Description], Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
        )
        SELECT TOP(@VariantNumberOfSalesPerDay)
            [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], @DateCounter, DATEADD(day, 1, @DateCounter), [Salesperson Key], [WWI Invoice ID], [Description], Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key]
        FROM [velv2].[seed_Sale]
        WHERE
             --[Sale Key] > @StartingSaleKey and /* IDENTITY DOES NOT WORK THE SAME IN SQLDW AND CAN'T USE THIS METHOD FOR VARIANT */
            [Invoice Date Key] >=cast(@YEAR AS CHAR(4)) + '-01-01'
        ORDER BY [Sale Key];

        SET @DateCounter = DATEADD(day, 1, @DateCounter);
    END;

END;
GO

EXEC [velv2].[InitialSalesDataPopulation]
GO
EXEC [velv2].[PopulateDateDimensionForYear] 2000
GO
EXEC [velv2].[Configuration_PopulateLargeSaleTable] 10, 2000
GO
SELECT MAX([Invoice Date Key]) FROM velv2.fact_Sale;
GO
EXEC sp_spaceused N'velv2.fact_Sale';
GO
/*
select * from velv2.dimension_Date
select * from velu.DimDate
*/
