#!/usr/bin/env python
# coding: utf-8

# ## Velu_03_fix_csv_python_notebook
# 
# sample fix csv file with line delimieter error, save to fixed dir 
# 

# read broken csv - bad row delimiter , only column as ,

# In[3]:


import numpy as np

rdd = sc.textFile(f'abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-poc/sale-20170502.csv')
#rdd = sc.textFile(f'abfss://csv@kzlongtermbackupstorage.dfs.core.windows.net/broken_CSV/broken_CSV.csv') 


# In[4]:


#read and split by ','
data = rdd.first().split(',')
# count rows 
field_count = len(data)
print(field_count)


# In[5]:


row_list = []
max_index = 11

# wiersz ma 11 kolumn
while max_index <= len(data):
    row = [data[i] for i in np.arange(max_index-11, max_index)]
    row_list.append(row)

    max_index += 11
 


# In[6]:


df_fixed = spark.createDataFrame(row_list,schema=['TransactionId', 'CustomerId', 'ProductId', 'Quantity', 'Price', 'TotalAmount', 'TransactionDateId', 'ProfitAmount', 'Hour', 'Minute', 'StoreId'])
display(df_fixed.limit(10))


# write to datalake   

# In[9]:


df_fixed.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save(f'abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-poc/sale-20170502-fixed2')
#df_fixed.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save(f'abfss://csv@kzlongtermbackupstorage.dfs.core.windows.net/fixedcsv/')


# In[12]:


mssparkutils.fs.ls("abfss://asa-kz@asadatalake19hvmew.dfs.core.windows.net/sale-poc")

