
CREATE SCHEMA velv2;
GO


---
/*
Dimension DISTRIBUTION = REPLICATE,
Facts     DISTRIBUTION = HASH([Movement Key])
*/
---
CREATE TABLE [velv2].[dimension_City]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_City]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_City]')
;

CREATE TABLE [velv2].[dimension_Customer]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_Customer]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_Customer]')
;

CREATE TABLE [velv2].[dimension_Employee]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_Employee]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_Employee]')
;

CREATE TABLE [velv2].[dimension_PaymentMethod]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_PaymentMethod]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_PaymentMethod]')
;

CREATE TABLE [velv2].[dimension_StockItem]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_StockItem]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_StockItem]')
;

CREATE TABLE [velv2].[dimension_Supplier]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_Supplier]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_Supplier]')
;

CREATE TABLE [velv2].[dimension_TransactionType]
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_TransactionType]
OPTION (LABEL = 'CTAS : Load [velv2].[dimension_TransactionType]')
;

CREATE TABLE [velv2].[fact_Movement]
WITH
(
    DISTRIBUTION = HASH([Movement Key]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_Movement]
OPTION (LABEL = 'CTAS : Load [velv2].[fact_Movement]')
;

CREATE TABLE [velv2].[fact_Order]
WITH
(
    DISTRIBUTION = HASH([Order Key]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_Order]
OPTION (LABEL = 'CTAS : Load [velv2].[fact_Order]')
;

CREATE TABLE [velv2].[fact_Purchase]
WITH
(
    DISTRIBUTION = HASH([Purchase Key]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_Purchase]
OPTION (LABEL = 'CTAS : Load [velv2].[fact_Purchase]')
;

CREATE TABLE [velv2].[seed_Sale]
WITH
(
    DISTRIBUTION = HASH([WWI Invoice ID]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_Sale]
OPTION (LABEL = 'CTAS : Load [velv2].[seed_Sale]')
;

CREATE TABLE [velv2].[fact_StockHolding]
WITH
(
    DISTRIBUTION = HASH([Stock Holding Key]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_StockHolding]
OPTION (LABEL = 'CTAS : Load [velv2].[fact_StockHolding]')
;

CREATE TABLE [velv2].[fact_Transaction]
WITH
(
    DISTRIBUTION = HASH([Transaction Key]),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[fact_Transaction]
OPTION (LABEL = 'CTAS : Load [velv2].[fact_Transaction]')
;