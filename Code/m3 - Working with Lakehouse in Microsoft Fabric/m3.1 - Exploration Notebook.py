#!/usr/bin/env python
# coding: utf-8

# ## Exploration Notebook
# 
# New notebook

# In[ ]:


"Hello World"


# In[ ]:


sum = 1 + 1

print(sum)


# In[ ]:


df = spark.sql("SELECT * FROM BronzeLakehouse.taxizones LIMIT 1000")
display(df)


# ### Exploration Notebook

# In[ ]:


df = spark.read.format("csv").option("header","true").load("Files/TaxiFilesUpload/Drivers.csv")
# df now is a Spark DataFrame containing CSV data from "Files/TaxiFilesUpload/Drivers.csv".
display(df)

