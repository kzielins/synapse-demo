#!/usr/bin/env python
# coding: utf-8

# ## Velu_10_Calculate Top10_ProductsUsers
# 
# Calculate Top 10 ProductsUsers
# TOP 10 Products overral
# 
# 
# l07_Ex1
# 

# Calculate top 10 products read from parquet and 
# result written to top10_product

# In[1]:


get_ipython().run_cell_magic('pyspark', '', "import uuid\r\n\r\n\r\ndf = spark.read.load('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/top-products/*.parquet', format='parquet')\r\ndisplay(df.limit(10))\n")


# In[ ]:


topPurchases = df.select(
    "UserId", 
    "ProductId",
    "ItemsPurchasedLast12Months",
    "IsTopProduct",
    "IsPreferredProduct")

# create temp  view top_purchases
topPurchases.createOrReplaceTempView("top_purchases")

topPurchases.show(11)


# In[ ]:


from pyspark.sql.functions import *

topPreferredProducts = (topPurchases
    .filter( col("IsTopProduct") == True)
    .filter( col("IsPreferredProduct") == True)
    .orderBy( col("ItemsPurchasedLast12Months").desc() ))

topPreferredProducts.show(11)


# Calcluate top 10 products, user

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\r\nCREATE OR REPLACE TEMPORARY VIEW top_10_productsUser\r\nAS\r\n    select \r\n        UserId,\r\n        ProductId, \r\n        ItemsPurchasedLast12Months\r\n    from (\r\n            select *,\r\n                row_number() over (partition by UserId order by ItemsPurchasedLast12Months desc) as rownum\r\n            from top_purchases\r\n        ) a\r\n    where \r\n        rownum <= 10 and \r\n        IsTopProduct == true and \r\n        IsPreferredProduct = true\r\n    order by\r\n        a.UserId\n')


# In[ ]:


top10ProductsUser = sqlContext.table("top_10_productsUser")
top10ProductsUser.show(11)


# In[ ]:


runId = uuid.uuid4()
top10ProductsUser.write.parquet('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/top10-productsUser/' + str(runId) + '.parquet')


# In[ ]:


print('topPreferredProducts(IsTopProduct=T,IsPreferredProduct=T): ', topPreferredProducts.count(), ', top10ProductsUser (filter: ', top10ProductsUser.count())


# Caclulate ALL top 10 products

# In[ ]:


top10ProductsOverall = (top10ProductsUser.select("ProductId","ItemsPurchasedLast12Months")
    .groupBy("ProductId")
    .agg( sum("ItemsPurchasedLast12Months").alias("Total") )
    .orderBy( col("Total").desc() )
    .limit(10))

top10ProductsOverall.show(11)


# Write calculated results  to datalake storage as parquet in top10-products dir 

# In[ ]:


runId = uuid.uuid4()
top10ProductsOverall.write.parquet('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/top10-products/' + str(runId) + '.parquet')

