#!/usr/bin/env python
# coding: utf-8

# ## Velu_02_sparc_profit
# 
# caclulate transakction, product profit  and save to sale-product-profit/' + str(runId) + '.parquet'
# 
# Read from sales reded from sale-small/*/*/*/*/*.parquet
# 

# In[4]:


get_ipython().run_cell_magic('pyspark', '', "df = spark.read.load('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-small/*/*/*/*/*.parquet', format='parquet')\r\ndisplay(df.limit(10))\n")


# In[5]:


df.createOrReplaceTempView("sale_small")


# In[6]:


get_ipython().run_cell_magic('sql', '', '\r\nCREATE OR REPLACE TEMPORARY VIEW sale_product_profit\r\nAS\r\n    select \r\n        TransactionDate,\r\n        ProductId, \r\n        sum(ProfitAmount) as sum_ProfitAmount,\r\n        avg(Quantity) as avg_Quantity,\r\n        sum(Quantity) as sum_Quantity\r\n    from sale_small\r\n    group by\r\n        TransactionDate,\r\n        ProductId\n')


# In[7]:


get_ipython().run_cell_magic('sql', '', 'select * from sale_product_profit\n')


# count product profit for transaction

# In[12]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

profitByDateProduct = (df.groupBy("TransactionDate","ProductId")
    .agg(
        sum("ProfitAmount").alias("sum_ProfitAmount"),
        round(avg("Quantity"), 4).alias("avg_Quantity"),
        sum("Quantity").alias("sum_Quantity"))
    .orderBy("TransactionDate"))
display(profitByDateProduct.limit(11))


# In[13]:


import uuid
runId = uuid.uuid4()
profitByDateProduct.write.parquet('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-product-profit/' + str(runId) + '.parquet')

