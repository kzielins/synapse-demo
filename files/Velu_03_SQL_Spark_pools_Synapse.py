#!/usr/bin/env python
# coding: utf-8

# ## Velu_03_SQL_Spark_pools_Synapse
# 
# Integrate SQLPoll with servlerless
# 
# 
# 
# L4_Ex4_
# 

# In[2]:


get_ipython().run_cell_magic('pyspark', '', '# integrate Synapse SQL (SQLPool1) with Spark Database Serverless\r\n# read using pyspark Spark Pools\r\n#1 View top_purchases created from purchares\r\nfrom pyspark.sql.functions import udf, explode\r\ndf = (spark.read \\\r\n        .option(\'inferSchema\', \'true\') \\\r\n        .json(\'abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/online-user-profiles-02/*.json\', multiLine=True)\r\n    )\r\nflat=df.select(\'visitorId\',explode(\'topProductPurchases\').alias(\'topProductPurchases_flat\'))\r\ntopPurchases = (flat.select(\'visitorId\',\'topProductPurchases_flat.productId\',\'topProductPurchases_flat.itemsPurchasedLast12Months\')\r\n    .orderBy(\'visitorId\'))\r\ntopPurchases.createOrReplaceTempView("top_purchases")\r\ndisplay(topPurchases.limit(10))\n')


# In[3]:


get_ipython().run_cell_magic('spark', '', '// Make sure the name of the dedcated SQL pool (SQLPool01 below) matches the name of your SQL pool.\r\n// Read from SQL to scala\r\nval df = spark.sqlContext.sql("select * from top_purchases")\r\ndf.describe()\r\ndisplay(df.limit(5))\n')


# In[4]:


get_ipython().run_cell_magic('spark', '', '//The Apache Spark pool to Synapse SQL connector is a data source implementation for Apache Spark. It uses the Azure Data Lake Storage Gen2 and PolyBase in dedicated SQL pools to efficiently transfer data between the Spark cluster and the Synapse SQL instance.\r\n// \r\n//write to SQLPool by synapsesql\r\ndf.write.synapsesql("SQLPool01.velu.TopPurchases", Constants.INTERNAL)\n')


# In[5]:


get_ipython().run_cell_magic('pyspark', '', "dfsales = spark.read.load('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-small/Year=2019/Quarter=Q4/Month=12/*/*.parquet', format='parquet')\r\ndisplay(dfsales.limit(10))\n")


# In[6]:


get_ipython().run_cell_magic('spark', '', '// Make sure the name of the SQL pool (SQLPool01 below) matches the name of your SQL pool.\r\nval df2 = spark.read.synapsesql("SQLPool01.velu.TopPurchases")\r\ndf2.createTempView("top_purchases_sql")\n')


# In[7]:


get_ipython().run_cell_magic('spark', '', 'df2.head(10)\r\ndf2.describe()\r\ndisplay(df2.limit(10))\n')


# In[8]:


get_ipython().run_cell_magic('pyspark', '', 'dfTopPurchasesFromSql = sqlContext.table("top_purchases_sql")\r\ndisplay(dfTopPurchasesFromSql.limit(5))\n')


# In[9]:


get_ipython().run_cell_magic('pyspark', '', 'from pyspark.sql.functions import *\r\ninner_join = dfsales.join(dfTopPurchasesFromSql,\r\n    (dfsales.CustomerId == dfTopPurchasesFromSql.visitorId) & (dfsales.ProductId == dfTopPurchasesFromSql.productId))\r\n\r\ninner_join_agg = (inner_join.select("CustomerId","TotalAmount","Quantity","itemsPurchasedLast12Months","top_purchases_sql.productId")\r\n    .groupBy(["CustomerId","top_purchases_sql.productId"])\r\n    .agg(\r\n        sum("TotalAmount").alias("TotalAmountDecember"),\r\n        sum("Quantity").alias("TotalQuantityDecember"),\r\n        sum("itemsPurchasedLast12Months").alias("TotalItemsPurchasedLast12Months"))\r\n    .orderBy("CustomerId") )\r\n\r\ndisplay(inner_join_agg.limit(100))\n')

