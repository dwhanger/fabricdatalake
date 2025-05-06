#!/usr/bin/env python
# coding: utf-8

# ## Cleaning Files with Vacuum
# 
# New notebook

# ### Check Audit History of Delta Table

# In[ ]:


DESCRIBE HISTORY YellowTaxis_OptimizeTest


# ### Vacuum: Dry Run

# In[ ]:


VACUUM YellowTaxis_OptimizeTest

RETAIN 0 HOURS DRY RUN


# ### Disable Retention Duration Check
# 
# <i>Currently, setting name include <b>databricks</b> (may or may not change in future)

# In[ ]:


SET spark.databricks.delta.retentionDurationCheck.enabled = false


# ### Vacuum
# With retention duration of zero hours

# In[ ]:


VACUUM YellowTaxis_OptimizeTest

RETAIN 0 HOURS


# In[ ]:




