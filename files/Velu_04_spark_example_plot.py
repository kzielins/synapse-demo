#!/usr/bin/env python
# coding: utf-8

# ## Velu_04_spark_example_plot
# 
# salse order sun by orderyear
# 
# Example plots
# 

# Sales order sum by orderyear (price+quantity)+tax
# 
# Example plots

# In[17]:


get_ipython().run_cell_magic('pyspark', '', 'from pyspark.sql.types import *\r\nfrom pyspark.sql.functions import *\r\n\r\n# read from csv - not serverless\r\norderSchema = StructType([\r\n    StructField("SalesOrderNumber", StringType()),\r\n    StructField("SalesOrderLineNumber", IntegerType()),\r\n    StructField("OrderDate", DateType()),\r\n    StructField("CustomerName", StringType()),\r\n    StructField("Email", StringType()),\r\n    StructField("Item", StringType()),\r\n    StructField("Quantity", IntegerType()),\r\n    StructField("UnitPrice", FloatType()),\r\n    StructField("Tax", FloatType())\r\n    ])\r\n\r\ndf = spark.read.load(\'abfss://files@asadatalake19hvmew.dfs.core.windows.net/sales/orders/*.csv\', format=\'csv\', schema=orderSchema)\r\ndisplay(df.limit(100))\n')


# In[8]:


df.printSchema()


# In[18]:


df.createOrReplaceTempView("salesorders")
spark_df = spark.sql("SELECT * FROM salesorders")
display(spark_df)


# In[19]:


get_ipython().run_cell_magic('sql', '', 'SELECT YEAR(OrderDate) AS OrderYear,\r\n       SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue\r\nFROM salesorders\r\nGROUP BY YEAR(OrderDate)\r\nORDER BY OrderYear;\n')


# In[20]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM salesorders\n')


# In[31]:


sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                 SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
             FROM salesorders \
             GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
             ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_spark.show()


# PlotLib

# In[36]:


from matplotlib import pyplot as plt

# Pandas dataframe
df_sales = df_spark.toPandas()


plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

# show
plt.show()


# In[41]:


import seaborn as sns
# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

