-- import to [velu].db.sales 
Use velu;
GO;
 

 CREATE EXTERNAL DATA SOURCE sales_data WITH (
     LOCATION = 'https://asadatalake19hvmew.dfs.core.windows.net/files/sales/'
 );
 GO;
CREATE EXTERNAL FILE FORMAT CsvFormat
     WITH (
         FORMAT_TYPE = DELIMITEDTEXT,
         FORMAT_OPTIONS(
         FIELD_TERMINATOR = ',',
         STRING_DELIMITER = '"'
         )
     );
 GO;
CREATE EXTERNAL FILE FORMAT Parquet
    WITH (FORMAT_TYPE = PARQUET);
GO
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://asadatalake19hvmew.dfs.core.windows.net/files/sales/*.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) WITH (
         SalesOrderNumber VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,
         SalesOrderLineNumber INT,
         OrderDate DATE,
         CustomerName VARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
         EmailAddress VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8,
         Item VARCHAR(30) COLLATE Latin1_General_100_BIN2_UTF8,
         Quantity INT,
         UnitPrice DECIMAL(18,2),
         TaxAmount DECIMAL (18,2)
     ) AS [result]
;
Go

SELECT *
 FROM
     OPENROWSET(
         BULK 'csv/*.csv',
         DATA_SOURCE = 'sales_data',
         FORMAT = 'CSV',
         PARSER_VERSION = '2.0'
     ) AS orders
GO

SELECT *
 FROM  
     OPENROWSET(
         BULK 'parquet/*.snappy.parquet',
         DATA_SOURCE = 'sales_data',
         FORMAT='PARQUET'
     ) AS orders
;

SELECT JSON_VALUE(Doc, '$.SalesOrderNumber') AS OrderNumber,
        JSON_VALUE(Doc, '$.CustomerName') AS Customer,
        Doc
 FROM
     OPENROWSET(
         BULK 'https://asadatalake19hvmew.dfs.core.windows.net/files/sales/json/',
         FORMAT = 'CSV',
         FIELDTERMINATOR ='0x0b',
         FIELDQUOTE = '0x0b',
         ROWTERMINATOR = '0x0b'
     ) WITH (Doc NVARCHAR(MAX)) as rows

CREATE EXTERNAL TABLE dbo.orders
 (
     SalesOrderNumber VARCHAR(10),
     SalesOrderLineNumber INT,
     OrderDate DATE,
     CustomerName VARCHAR(25),
     EmailAddress VARCHAR(50),
     Item VARCHAR(30),
     Quantity INT,
     UnitPrice DECIMAL(18,2),
     TaxAmount DECIMAL (18,2)
 )
 WITH
 (
     DATA_SOURCE =sales_data,
     LOCATION = 'csv/*.csv',
     FILE_FORMAT = CsvFormat
 );
 GO


CREATE EXTERNAL TABLE dbo.orders_parquet 
 (
     SalesOrderNumber VARCHAR(10),
     SalesOrderLineNumber INT,
     OrderDate DATE,
     CustomerName VARCHAR(25),
     EmailAddress VARCHAR(50),
     Item VARCHAR(30),
     Quantity INT,
     UnitPrice DECIMAL(18,2),
     TaxAmount DECIMAL (18,2)
 )
 WITH
 (
	    DATA_SOURCE= sales_data,
         LOCATION = 'parquet/*.snappy.parquet',
         FILE_FORMAT = parquet
)
GO
