#!/usr/bin/env python
# coding: utf-8

# ## Auto Optimization and V-Order Test
# 
# New notebook

# ### A. Without optimization settings

# #### A.1 Disable optimization settings

# In[ ]:


# Optimize Write: Disable
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "false")

# Auto Compaction: Disable
spark.conf.set("spark.microsoft.delta.autoCompact.enabled", "false")

# V-Order: Disable
spark.conf.set("spark.sql.parquet.vorder.enabled", "false")


# #### A.2 Load a new table: Non-Optimized
# 
# All optimization settings are disabled

# In[ ]:


(
    spark
        .read
        .table("yellowtaxis")

        .repartition(4)
        
        .write
        .mode("append")        

        .saveAsTable("YellowTaxis_NonOptimized")
)


# ### B. With auto-optimization settings

# #### B.1 Enable auto-optimization settings, disable V-Order

# In[ ]:


# Optimize Write: Enable
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# Auto Compaction: Enable
spark.conf.set("spark.microsoft.delta.autoCompact.enabled", "true")

# V-Order: Disable
spark.conf.set("spark.sql.parquet.vorder.enabled", "false")


# #### B.2 Load a new table: Auto-Optimized
# 
# Auto-optimization is enabled, V-Order is disabled

# In[ ]:


(
    spark
        .read
        .table("yellowtaxis")

        .repartition(4)
        
        .write
        .mode("append")       

        .saveAsTable("YellowTaxis_AutoOptimized")
)


# ### C. With optimization settings

# #### C.1 Enable auto-optimization and V-Order

# In[ ]:


# Optimize Write: Enable
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# Auto Compaction: Enable
spark.conf.set("spark.microsoft.delta.autoCompact.enabled", "true")

# V-Order: Enable
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")


# #### C.2 Load a new table: Auto-Optimized & V-Order
# 
# Auto-optimization is enabled, V-Order is enabled

# In[ ]:


(
    spark
        .read
        .table("yellowtaxis")

        .repartition(4)
        
        .write
        .mode("append")        

        .saveAsTable("YellowTaxis_AutoOptimized_VOrder")
)


# In[ ]:




