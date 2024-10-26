from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import *
import pandas as pd

#creating session
spark = SparkSession.builder.getOrCreate()

#reading csv into spark dataframe
df = spark.read.csv("apartmentsclean.csv", header=True, sep=',', inferSchema=True)
#df.show()

#doing correlations
c1 = df.stat.corr("bedrooms","price")
c2 = df.stat.corr("bathrooms","price")
c3 = df.stat.corr("square_feet","price")
print("bedrooms vs price correlation")
print(c1)
print("bathrooms vs price correlation")
print(c2)
print("square feet vs bathrooms correlation")
print(c3)

#states array
states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']


#sva = states vs. average price per state dataframe
#creating 2d array with just states and their average apartment price
svaArray = []
for st in states:
	svaArraydf = df.where(df.state==st)
	svaAdd = [st, svaArraydf.select(avg("price")).collect()[0][0]]
	svaArray.append(svaAdd)

#creating pandas dataframe to convert to spark dataframe
pandadf = pd.DataFrame(svaArray, columns = ['state','averagePrice'])
sva = spark.createDataFrame(pandadf)

#getting state with highest and lowest average apartment price
svaMax = sva.select(max(col("averagePrice"))).collect()[0][0]
print("The state with the highest average apartment price is shown below")
sva.where(sva["averagePrice"] == svaMax).show()

svaMin = sva.select(min(col("averagePrice"))).collect()[0][0]
print("The state with the lowest average apartment price is shown below")
sva.where(sva["averagePrice"] == svaMin).show()


