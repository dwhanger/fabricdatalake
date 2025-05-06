#!/usr/bin/env python
# coding: utf-8

# ## Optimization
# 
# New notebook

# ### 1. Disable optimization settings

# In[ ]:


spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "false")

spark.conf.set("spark.microsoft.delta.autoCompact.enabled", "false")

spark.conf.set('spark.sql.parquet.vorder.enabled', 'false')


# ### 2. Read YellowTaxis table

# In[ ]:


yellowTaxiDF = (
                    spark
                        .read
                        .table("yellowtaxis")
               )


# ### 3. Load YellowTaxis data to a new table five times

# In[ ]:


for x in range(5):  
    (
        yellowTaxiDF
            .write

            .mode("append")

            .partitionBy("PickupLocationId")                      

            .saveAsTable("YellowTaxis_OptimizeTest")
    )


# ### 4. Check performance before optimization

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nSELECT PickupLocationId\n     , COUNT(*) AS TotalRides\n\nFROM YellowTaxis_OptimizeTest\n\nWHERE TripYear = 2024\n  AND TripMonth = 5\n  AND TripDay = 15\n\nGROUP BY PickupLocationId\n\nORDER BY TotalRides DESC\n')


# ### 5. Optimize: Bin-packing

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nOPTIMIZE YellowTaxis_OptimizeTest\n\nWHERE PickupLocationId = 100              -- optional\n')


# ### 6. Optimize: Z-Ordering

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nOPTIMIZE YellowTaxis_OptimizeTest\n\nZORDER BY TripYear, TripMonth, TripDay    -- optional\n')


# ### 7. Check performance after optimization

# In[ ]:


get_ipython().run_cell_magic('sql', '', '\nSELECT PickupLocationId\n     , DropLocationId\n     , COUNT(*) AS TotalRides\n\nFROM YellowTaxis_OptimizeTest\n\nWHERE TripYear = 2024\n    AND TripMonth = 5\n    AND TripDay = 15\n\nGROUP BY PickupLocationId, DropLocationId\n\nORDER BY TotalRides DESC\n')


# In[ ]:




