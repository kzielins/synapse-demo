#!/usr/bin/env python
# coding: utf-8

# ## Velu_09_json_sort_explode_save
# 
# reading json file for User_prrifiles 
# explode json elements (productId, itemsPurchased last 12 mc) 
# Agregates by visitor/userid , with count tocal visits,
# 
# Agregates by visitor/userid , with sum purchasedvisits,
# ,  
# 
# Writes results to 
# /sum-purchases/' + str(runId) + '.parquet' , 
# /count-purchases/' + str(runId) + '.parquet'
# 

# reading json file for User_prrifiles 
# explode json elements (productId, itemsPurchased last 12 mc) 
# Agregates by visitor/userid , with count tocal visits,
# 
# Agregates by visitor/userid , with sum purchasedvisits,
# ,  
# 
# Writes results to 
# /sum-purchases/' + str(runId) + '.parquet' , 
# /count-purchases/' + str(runId) + '.parquet'

# In[4]:


df = (spark.read \
        .option('inferSchema', 'true') \
        .json('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/online-user-profiles-02/*.json', multiLine=True)
    )

df.printSchema()


# In[5]:


df.createOrReplaceTempView("user_profiles")


# In[6]:


get_ipython().run_cell_magic('sql', '', '\r\nSELECT * FROM user_profiles LIMIT 10\n')


# explode json 

# In[7]:


from pyspark.sql.functions import udf, explode

flat=df.select('visitorId',explode('topProductPurchases').alias('topProductPurchases_flat'))
#flat.show(100)


# In[8]:


topPurchases = (flat.select('visitorId','topProductPurchases_flat.productId','topProductPurchases_flat.itemsPurchasedLast12Months')
    .orderBy('visitorId'))

#topPurchases.show(10)
df.createOrReplaceTempView("topPurchases")


# In[9]:


# order  
sortedTopPurchases = topPurchases.orderBy("itemsPurchasedLast12Months")
display(sortedTopPurchases.limit(10))
df.createOrReplaceTempView("sortedTopPurchases")


# In[13]:


from pyspark.sql.functions import *
sortedTopPurchasesDesc = (topPurchases.orderBy( col("itemsPurchasedLast12Months").desc() ))
display(sortedTopPurchasesDesc.limit(10))


# In[14]:


get_ipython().run_cell_magic('sql', '', 'select * from topPurchases\n')


# count purchases for each visitor

# In[15]:


#10
groupedTopPurchases = (sortedTopPurchases.select("visitorId")
    .groupBy("visitorId")
    .agg(count("*").alias("total"))
    .orderBy("visitorId") )

display(groupedTopPurchases.limit(100))


# In[16]:


groupedSumPurchases = (sortedTopPurchases.select("visitorId","itemsPurchasedLast12Months")
    .groupBy("visitorId")
    .agg(sum("itemsPurchasedLast12Months").alias("totalItemsPurchased"))
    .orderBy("visitorId") )

display(groupedSumPurchases.limit(100))


# In[17]:


get_ipython().run_cell_magic('pyspark', '', "import uuid\r\n# Generate random GUID\r\nrunId = uuid.uuid4()\r\ngroupedSumPurchases.write.parquet('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sum-purchases/' + str(runId) + '.parquet')\r\ngroupedTopPurchases.write.parquet('abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/count-purchases/' + str(runId) + '.parquet')\n")

