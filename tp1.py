

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

#witout dataframes
f =  sc.wholeTextFiles('input1') # (k, v) = (path/filename.txt, file content)
f1 = f.map(lambda (k, v): (k.split("/")[-1] ,v)) # (k, v) = (filename.txt, file content)
f2 = f1.map(lambda (k, v): (k.split(".txt")[0] ,v)) # (k, v) = (filename(storeName), file content)
f3 = f2.flatMapValues(lambda v: v.split("\r\n")) # (k, v) = (storeName, v= 'Mon Rev') # an entry for each month
f4= f3.map(lambda (k, v): (k, k.split("_")[0], v.split(" ")[0], float(v.split(" ")[1] ))) # (store, city, mon, rev)
# 1 - Average monthly income of the shop in France
total_revenue= f4.map(lambda (s, c, m, r): r).reduce(lambda x, y: x+y)
print ('average monthly income is {0:.3f} k$'.format(total_revenue/12))
# 3 - Total revenue per city per year
cityAsKey_r = f4.map(lambda (s, c, m, r): (c, r)) # RDD (city, revenue)
totalRev_city_annual=cityAsKey_r.reduceByKey(lambda x, y : x+y) 
totalRev_city_annual.collect() 
# 2 - Average monthly income of the shop in each city
# based on annual income:
avgMonthlyIncomePerCity=totalRev_city_annual.mapValues(lambda v: v/float(12))
avgMonthlyIncomePerCity.collect()
#or for better microscopic vision, we can calculate the average 
monthCityAsKey_r = f4.map(lambda (s, c, m, r): (m+'_'+c, float(r) ))
combiner=(lambda x: (x, 1))
mergeValue = (lambda x, y: (x[0] + y, x[1]+1))
mergeComb = (lambda x, y: (x[0]+y[0], x[1] + y[1] ))
avgMonthlyIncomePerCity_Micr = monthCityAsKey_r.combineByKey(combiner, mergeValue, mergeComb)
avgMonthlyIncomePerCity_Micr.mapValues(lambda (x, y):x/y)\
   ....:              .sortByKey()\
   ....:              .map(lambda (k, v): (k.split('_')[0], k.split('_')[1], v))\
   ....:              .saveAsTextFile('outputMonthlyAvgIncome3')
#4. Total revenue per store per year
storeAsKey_rev = f4.map(lambda (s, c, m, r): (s, r))
totalRev_store_annual=storeAsKey_rev.reduceByKey(lambda x,y :x+y)
totalRev_store_annual.collect()
#5. The store that achieves the best performance in each month
monthAsKey = f4.map(lambda (s, c, m, r): (m, s, float(r)))
monthAsKeyPairS_R= monthAsKey.map(lambda (x, y, z): (x, (y, z)))
def compare(x, y):
	if x[1]<y[1]: return y
	else: return x
combiner=(lambda x: (x[0], x[1]))
mergeValue = (lambda x, y: compare(x, y))
mergeComb = (lambda x, y: compare(x, y))
bestEveryMonth = monthAsKeyPairS_R.combineByKey(combiner, mergeValue, mergeComb)
bestEveryMonth.collect()
#1. using data frames	
f5 = f4.map(lambda (s, c, m, r): (s, c, m, float(r)))
df=sqlCtx.createDataFrame(f5, ['store', 'city', 'month', 'revenue'])
totalIncome = df.groupBy().sum()
avg = totalIncome.first()[0]/12
#3. Total revenue per city per year
totalIncomePerCityPerYear = df.groupBy(df.city).sum()
totalIncomePerCityPerYear.show()

#create a view 
df.createOrReplaceTempView("income")

#2. Average monthly income of the shop in each city
sqlCtx.sql('select city, sum(revenue)/12 as sum from income group by city order by sum desc').show()
#4. Total revenue per store per year
sqlCtx.sql('select store, sum(revenue) as sum from income group by store order by sum desc').show()
#5. The store that achieves the best performance in each month
sqlCtx.sql("SELECT month, store, revenue from income join (SELECT month m, max(revenue) maxRevenue FROM  income group by month) t on month=t.m AND revenue=t.maxRevenue").show()




















