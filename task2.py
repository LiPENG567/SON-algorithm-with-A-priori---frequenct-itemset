#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from itertools import permutations
import itertools
import json
import sys
import time
start = time.time()
k = int(sys.argv[1])
s = int(sys.argv[2])
input_path = sys.argv[3]
out_path = sys.argv[4]


# sc.stop()
sc = pyspark.SparkContext()
rdd = sc.textFile(input_path)
header = rdd.first()
rdd_d = rdd.filter(lambda a: a!= header)
def counteach(chunck, candset):
    chunck = list(chunck)
    result = list()
    for i in candset:
        cnt=0
        for li in chunck:
            if(set(i).issubset(li)):
                cnt +=1
        result.append([i,cnt])

    return result 


def Apri(allL,itemset,support):
    
    itemslist = itemset
    result=list()
    k=1
    size1_set=[]
    list1d = list(itertools.chain(*itemslist))
    uniq = set(list1d)
    for i in uniq:
        cnt = list1d.count(i)
        if cnt >= support:
            size1_set.append(i)
    size1_setm = [[x] for x in size1_set]
    result.extend(size1_setm) 
    
    k = k+1
    
    size2_set=list()
    par = []
    for c in itertools.combinations(size1_set,2):
        par = list(c)
        par.sort()
        size2_set.append(par)
    size2_set.sort()
    size2_true=countfreq(itemslist,size2_set,support) 
    freqst = size2_true
    freqst.sort()
    result.extend(freqst)
    

    k += 1

    # generate new comb and count with loop
    while (len(freqst)!=0):
        sizek_true=[]
#         print('k is ', k)
        cand = generateCK(freqst,k)
#         print('cand i am looing now is ', cand)
        if len(cand)==0:
            break
        cand.sort()
        sizek_true = countfreq(itemslist,cand,support) 
        sizek_true.sort()
#         print('size 3 is ', sizek_true)
        result.extend(sizek_true)
#         print('now3 is ',result)
        freqst = sizek_true
#         print('length is ',len(freqst))
        k += 1       
    return result

def generateCK(Lk,k):
    ### obtain the combination with larger than 1 comb from pruned list
    cands= list()
  
    for i in range(len(Lk)-1):
        for j in range(i+1,len(Lk)):
            
            if(Lk[i][0:k-2] == Lk[j][0:k-2]) :
                cand = list(set(Lk[i])|set(Lk[j]))
                cand.sort()
                if(cand not in cands):
                    cands.append(cand)
            else:
                break
    return cands

# def countfreq(newset,origlist):
def countfreq(origlist,newset,support):
    result = []
    for i in newset:
        cnt=0
        for li in origlist:
            if(set(i).issubset(li)):
                cnt +=1
        if cnt >= support:
            result.append(i)
           
    return result 


Myrdd = rdd_d.map(lambda line: (line.split(',')[0], line.split(',')[1])).persist()
n_partition = Myrdd.getNumPartitions()
#     s = 8 # support threshhold
s1 = s/n_partition
myrdd = Myrdd.distinct().groupByKey().map(lambda a: (a[0],list(a[1]))).filter(lambda a: len(a[1])>int(k))
items_rdd = myrdd.map(lambda a:a[1])
list_itemrdd = items_rdd.collect()
# print('list of item is ', list_itemrdd)
n_item = items_rdd.glom().map(len).collect()
#     print('n_item is ', n_item)
# print('I am now is list', items_rdd.take(500), items_rdd.count())  

SONphase1map = items_rdd.mapPartitions(lambda a: Apri(a,list_itemrdd,s1)).map(lambda a: (tuple(a),1))
# print('SON test is ',SONphase1map.take(10))
SONphase1reduce = SONphase1map.distinct().sortByKey().map(lambda a: a[0]).collect()
# print('SON test reduce is ',SONphase1reduce)
SONphase2map = items_rdd.mapPartitions(lambda a:counteach(a,SONphase1reduce))
# print('SON phase2 map is ',SONphase2map.take(5))
SONphase2reduce = SONphase2map.reduceByKey(lambda a,b: (a+b)).filter(lambda a: a[1]>=s).sortByKey().map(lambda a: a[0]).collect()
# print('SON phase2 reduce is ',SONphase2reduce)

# preprocess the output
outlist = SONphase1reduce
lenmx = 0
for i in outlist:
    if len(i) > lenmx:
        lenmx = len(i)
print(lenmx)

allout = []
for l in range(1,lenmx+1):
    listc=[]
    for a in outlist:
        if len(a)==l:
            listc.append(a)
    allout.append(listc)

# preprocess the true frequenct itemset
fset = SONphase2reduce
lenm = 0
for i in fset:
    if len(i) > lenm:
        lenm = len(i)
print(lenm)

freqtr = []
for l in range(1,lenm+1):
    listc=[]
    for a in fset:
        if len(a)==l:
            listc.append(a)
    freqtr.append(listc)


filename = out_path
with open(filename,'w') as zaili:
    zaili.write("Candidates:"+ '\n')
    mm = len(allout[0])
    for i in range(mm):
        nn = allout[0][i]
        new_i = str(nn[0])
        if i == mm-1:
            zaili.write('('+"'"+new_i+"'"+')')
        else:
            zaili.write('('+"'"+new_i+"'"+')'+',')
    zaili.write("\n\n")
    for i in range(1,lenmx):
        bb = allout[i]
        for j in range(len(bb)):
            if j == len(bb)-1:
                zaili.write(str(bb[j]))
            else:
                zaili.write(str(bb[j])+',')
        zaili.write("\n\n")
        
#     zaili.write("\n\n")
    zaili.write("Frequent Itemsets:"+ '\n')
    mm = len(freqtr[0])
    for i in range(mm):
        nn = freqtr[0][i]
        new_i = str(nn[0])
        if i == mm-1:
            zaili.write('('+"'"+new_i+"'"+')')
        else:
            zaili.write('('+"'"+new_i+"'"+')'+',')    
    
    for i in range(1,lenm):
        zaili.write("\n\n")
        bb = freqtr[i]
        for j in range(len(bb)):
            if j == len(bb)-1:
                zaili.write(str(bb[j]))
            else:
                zaili.write(str(bb[j])+',')
#         zaili.write("\n\n") 
end = time.time()
total = end-start
print('Duration:',round(total,2))

