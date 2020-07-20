#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pyspark
import json
import sys
import time
import csv
start = time.time()

sc.stop()
sc = pyspark.SparkContext()
review = sc.textFile(input path)
RErdd = review.map(json.loads).map(lambda row: (row['business_id'],row['user_id'])).persist()
# STrdd = RErdd.groupByKey().map(lambda a: (a[0], (sum(a[1]), len(a[1]))))

business = sc.textFile('/Users/zailipeng/Desktop/my_research/Important_information/books/CS/Inf553/HW2/business.json')
BUrdd = business.map(json.loads).map(lambda row: (row['business_id'],row['state'])).filter(lambda a: a[1]=='NV').map(lambda a:(a[0],1)).persist()
# Catrdd = BUrdd.filter(lambda a: (a[1] is not None) and (a[1] is not "")).mapValues(lambda a: [ai.strip() for ai in a.split(',')])
# print(BUrdd.take(5))

JOrdd = BUrdd.leftOuterJoin(RErdd).map(lambda a: (a[1][1],a[0])).filter(lambda a: a[0] is not None).collect() # change to userID and business ID
# print(JOrdd[0:60])

with open ("user_business.csv", 'w') as csvfile:
    headers = ['user_id', 'business_id']
    writer = csv.writer(csvfile)
    writer.writerow(headers)
    for i in JOrdd:
        writer.writerow((i))
        
end = time.time()
total = end-start
print('Duration:',round(total,2))


# In[ ]:





# In[ ]:




