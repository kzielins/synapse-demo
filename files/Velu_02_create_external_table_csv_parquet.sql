IF NOT EXISTS (SELECT * FROM sys.symmetric_keys) BEGIN
    declare @pasword nvarchar(400) = CAST(newid() as VARCHAR(400));
    EXEC('CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''' + @pasword + '''')
END

CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'sv=2018-03-28&ss=bf&srt=sco&sp=rl&st=2019-10-14T12%3A10%3A25Z&se=2061-12-31T12%3A10%3A00Z&sig=KlSU2ullCscyTS0An0nozEpo4tO5JAgGBvw%2FJX2lguw%3D'
GO

-- Create external data source secured using credential
CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH (
    LOCATION = 'https://sqlondemandstorage.blob.core.windows.net',
    CREDENTIAL = sqlondemand
);
GO

CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeader
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);
GO

CREATE EXTERNAL TABLE [population]
(
    [country_code] VARCHAR (5) COLLATE Latin1_General_BIN2,
    [country_name] VARCHAR (100) COLLATE Latin1_General_BIN2,
    [year] smallint,
    [population] bigint
)
WITH (
    LOCATION = 'csv/population/population.csv',
    DATA_SOURCE = SqlOnDemandDemo,
    FILE_FORMAT = QuotedCsvWithHeader
);
GO

---- export to gen2 storage
CREATE DATABASE SCOPED CREDENTIAL [SasTokenWriteWWI]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
     SECRET = 'st=2023-07-15T16:21:26Z&se=2028-07-16T00:21:26Z&si=kzz&spr=https&sv=2022-11-02&sr=c&sig=cJhI7pKAc8nBKRGYPiOkil0kdWvfg8FgWSy%2Bwzf83Bg%3D';
GO
CREATE EXTERNAL DATA SOURCE [MyDataSourceWWI] WITH (
    LOCATION = 'https://kzlongtermbackupstorage.blob.core.windows.net/wwi', CREDENTIAL = [SasTokenWriteWWI]
);
GO

CREATE EXTERNAL FILE FORMAT parquetfile 
WITH ( FORMAT_TYPE = PARQUET, DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec' );
GO
 
CREATE EXTERNAL FILE FORMAT QuotedCsvWithHeader
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);
GO
CREATE EXTERNAL FILE FORMAT QuotedCsvWithoutHeader
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 1
    )
);
GO 


CREATE EXTERNAL TABLE populationsCSV
WITH (
	LOCATION = '/csv/population',
	DATA_SOURCE = MyDataSourceWWI,
	FILE_FORMAT = QuotedCsvWithHeader
	)
AS
select * from population
GO

CREATE EXTERNAL TABLE populationParquet
WITH (
	LOCATION = '/parquet/population',
	DATA_SOURCE = MyDataSourceWWI,
	FILE_FORMAT = parquetfile
	)
AS
select * from population
GO


-------


SELECT [country_code]
    ,[country_name]
    ,[year]
    ,[population]
FROM [dbo].[population]
WHERE [year] = 2019 and population > 100000000

---
CREATE VIEW CustomerInfo AS
    SELECT * 
FROM OPENROWSET(
        BULK 'https://asadatalake19hvmew.dfs.core.windows.net/asa-kz/customer-info/customerinfo.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        FIRSTROW=2
    )
    WITH (
    [UserName] NVARCHAR (50),
    [Gender] NVARCHAR (10),
    [Phone] NVARCHAR (50),
    [Email] NVARCHAR (100),
    [CreditCard] NVARCHAR (50)
    ) AS [r];
    GO

SELECT * FROM CustomerInfo;
GO
