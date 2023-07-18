-- ================
-- StageProduct
-- ================


SELECT count(1) FROM velu.StageProduct
--- load data
truncate table velu.StageProduct

--- load data

COPY INTO velu.StageProduct
    (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
FROM 'https://asadatalake19hvmew.blob.core.windows.net/files/data/Product.csv'
WITH
(
    FILE_TYPE = 'CSV',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF',
    FIRSTROW = 2 ,
    ERRORFILE = 'https://asadatalake19hvmew.dfs.core.windows.net/files/errors/Product/'
);

SELECT count(1) FROM velu.StageProduct;


-- ================
 -- StageCustomer;
-- ================
SELECT count(1) FROM velu.StageCustomer;
truncate table velu.StageCustomer;
COPY INTO velu.StageCustomer
(GeographyKey, CustomerAlternateKey, Title, FirstName, MiddleName, LastName, NameStyle, BirthDate, 
MaritalStatus, Suffix, Gender, EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome, EnglishEducation, 
SpanishEducation, FrenchEducation, EnglishOccupation, SpanishOccupation, FrenchOccupation, HouseOwnerFlag, 
NumberCarsOwned, AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance)
FROM 'https://asadatalake19hvmew.dfs.core.windows.net/files/data/Customer.csv'
WITH
(
FILE_TYPE = 'CSV'
,MAXERRORS = 5
,FIRSTROW = 2 -- skip header row
,ERRORFILE = 'https://asadatalake19hvmew.dfs.core.windows.net/files/errors/Customer/'
);
SELECT count(1) FROM velu.StageCustomer;


-- ================
--DimProduct V2 as  CREATE TABLE AS (CTAS) statement
-- ================
CREATE TABLE velu.DimProductV2
WITH
(
    DISTRIBUTION = HASH(ProductAltKey),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY ProductID) AS ProductKey,
    ProductID AS ProductAltKey,
    ProductName,
    ProductCategory,
    Color,
    Size,
    ListPrice,
    Discontinued
FROM velu.StageProduct;


-- ================
-- Slowy change dimension TYPe 1 i 2 (SCD1 , SCD2)
-- ================
INSERT INTO velu.DimCustomer ([GeographyKey],[CustomerAlternateKey],[Title],[FirstName],[MiddleName],[LastName],[NameStyle],[BirthDate],[MaritalStatus],
[Suffix],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],[EnglishEducation],[SpanishEducation],[FrenchEducation],
[EnglishOccupation],[SpanishOccupation],[FrenchOccupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],[AddressLine2],[Phone],
[DateFirstPurchase],[CommuteDistance])
SELECT *
FROM velu.StageCustomer AS stg
WHERE NOT EXISTS
    (SELECT * FROM velu.DimCustomer AS dim
    WHERE dim.CustomerAlternateKey = stg.CustomerAlternateKey);

-- Type 1 updates (change name, email, or phone in place)
UPDATE velu.DimCustomer
SET LastName = stg.LastName,
    EmailAddress = stg.EmailAddress,
    Phone = stg.Phone
FROM velu.DimCustomer dim inner join velu.StageCustomer stg
ON dim.CustomerAlternateKey = stg.CustomerAlternateKey
WHERE dim.LastName <> stg.LastName OR dim.EmailAddress <> stg.EmailAddress OR dim.Phone <> stg.Phone

-- Type 2 updates (address changes triggers new entry)
INSERT INTO velu.DimCustomer
SELECT stg.GeographyKey,stg.CustomerAlternateKey,stg.Title,stg.FirstName,stg.MiddleName,stg.LastName,stg.NameStyle,stg.BirthDate,stg.MaritalStatus,
stg.Suffix,stg.Gender,stg.EmailAddress,stg.YearlyIncome,stg.TotalChildren,stg.NumberChildrenAtHome,stg.EnglishEducation,stg.SpanishEducation,stg.FrenchEducation,
stg.EnglishOccupation,stg.SpanishOccupation,stg.FrenchOccupation,stg.HouseOwnerFlag,stg.NumberCarsOwned,stg.AddressLine1,stg.AddressLine2,stg.Phone,
stg.DateFirstPurchase,stg.CommuteDistance
FROM velu.StageCustomer AS stg
JOIN velu.DimCustomer AS dim
ON stg.CustomerAlternateKey = dim.CustomerAlternateKey
AND stg.AddressLine1 <> dim.AddressLine1;

-- ================
-- DHW OPT
-- ================
ALTER INDEX ALL ON velu.DimProduct REBUILD;

CREATE STATISTICS customergeo_stats ON velu.DimCustomer (GeographyKey);
-- ================
-- ================
-- ================
-- ================

