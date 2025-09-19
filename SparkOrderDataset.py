#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark= SparkSession. builder. config('spark.ui.port','0'). config('spark.shuffle.useOldFetchProtocol', 'true'). config("spark.sql.warehouse.dir", f"/user/itv018265/warehouse"). enableHiveSupport(). master('yarn'). getOrCreate()


# In[3]:


spark


# In[4]:


orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_base.take(10)


# In[6]:


orders_mapped = orders_base.map(lambda x: (x.split(",")[2],x.split(",")[3]))


# In[7]:


customers_base=spark.sparkContext.textFile("/public/trendytech/retail_db/customers/part-00000")
customers_base.take(10)


# In[8]:


customers_mapped = customers_base.map(lambda x: (x.split(",")[0],x.split(",")[8]))


# In[9]:


joined_rdd = customers_mapped.join(orders_base)


# In[10]:


joined_rdd.saveAsTextFile("data/orders_joined")


# In[11]:


spark.stop()


# In[4]:


orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_base.take(10)


# In[8]:


orders_mapped = orders_base.map(lambda x: (x.split(",")[2],x.split(",")[3]))


# In[6]:


customers_base=spark.sparkContext.textFile("/public/trendytech/retail_db/customers/part-00000")
customers_base.take(10)


# In[7]:


customers_mapped = customers_base.map(lambda x: (x.split(",")[0],x.split(",")[8]))


# In[9]:


customers_broadcast = spark.sparkContext.broadcast(customers_mapped.collect())


# In[10]:


def get_pincode(customer_id):
    try:
        return customers_broadcast.value[customer_id]
    except:
        return "-1"


# In[12]:


joined_rddd = orders_mapped.map(lambda x: (get_pincode(int(x[0])),x[1]))


# In[13]:


joined_rddd.take(10)


# In[15]:


joined_rddd.saveAsTextFile("data/broadcastResults")


# In[ ]:




