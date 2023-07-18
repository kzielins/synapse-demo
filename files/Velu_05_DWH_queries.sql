 -- query Internet sales 
 -- aprox count vs precise count - 2% diff
 SELECT d.CalendarYear AS CalendarYear,
     APPROX_COUNT_DISTINCT(i.SalesOrderNumber) AS Aprox_Orders ,COUNT(DISTINCT i.SalesOrderNumber) AS Orders
 FROM velu.FactInternetSales AS i
 JOIN velu.DimDate AS d ON i.OrderDateKey = d.DateKey
 GROUP BY d.CalendarYear
 ORDER BY CalendarYear;

 

-- sales by year,month
 SELECT  d.CalendarYear AS Year,
         d.MonthNumberOfYear AS Month,
         SUM(i.SalesAmount) AS InternetSalesAmount
 FROM velu.FactInternetSales AS i
 JOIN velu.DimDate AS d ON i.OrderDateKey = d.DateKey
 GROUP BY d.CalendarYear, d.MonthNumberOfYear
 ORDER BY Year, Month;


 --- DWH snowdlake 
 --- sales by year, produc categgory region, sum sales

 SELECT  d.CalendarYear AS Year,
         pc.EnglishProductCategoryName AS ProductCategory,
         g.EnglishCountryRegionName AS Region,
         SUM(i.SalesAmount) AS InternetSalesAmount
 FROM velu.FactInternetSales AS i
 JOIN velu.DimDate AS d ON i.OrderDateKey = d.DateKey
 JOIN velu.DimCustomer AS c ON i.CustomerKey = c.CustomerKey
 JOIN velu.DimGeography AS g ON c.GeographyKey = g.GeographyKey
 JOIN velu.DimProduct AS p ON i.ProductKey = p.ProductKey
 JOIN velu.DimProductSubcategory AS ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
 JOIN velu.DimProductCategory AS pc ON ps.ProductCategoryKey = pc.ProductCategoryKey
 GROUP BY d.CalendarYear, pc.EnglishProductCategoryName, g.EnglishCountryRegionName
 ORDER BY Year, ProductCategory, Region;



 --- region sales  with rank DESCending 
 select * from (
    SELECT  g.EnglishCountryRegionName AS Region,
            ROW_NUMBER() OVER(PARTITION BY g.EnglishCountryRegionName
                            ORDER BY i.SalesAmount DESC) AS RowNumber,
            i.SalesOrderNumber AS OrderNo,
            i.SalesOrderLineNumber AS LineItem,
            i.SalesAmount AS SalesAmount,
            SUM(i.SalesAmount) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionTotal,
            AVG(i.SalesAmount) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionAverage
    FROM velu.FactInternetSales AS i
    JOIN velu.DimDate AS d ON i.OrderDateKey = d.DateKey
    JOIN velu.DimCustomer AS c ON i.CustomerKey = c.CustomerKey
    JOIN velu.DimGeography AS g ON c.GeographyKey = g.GeographyKey
    )g
 where RowNumber=1
 ORDER BY Region


 --- rank the cities in each region based on their total sales amount:
  SELECT  g.EnglishCountryRegionName AS Region,
         g.City,
         SUM(i.SalesAmount) AS CityTotal,
         SUM(SUM(i.SalesAmount)) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionTotal,
         RANK() OVER(PARTITION BY g.EnglishCountryRegionName
                     ORDER BY SUM(i.SalesAmount) DESC) AS RegionalRank
 FROM velu.FactInternetSales AS i
 JOIN velu.DimDate AS d ON i.OrderDateKey = d.DateKey
 JOIN velu.DimCustomer AS c ON i.CustomerKey = c.CustomerKey
 JOIN velu.DimGeography AS g ON c.GeographyKey = g.GeographyKey
 GROUP BY g.EnglishCountryRegionName, g.City
 ORDER BY Region;




 