import findspark

findspark.init()

#import os



from pyspark.sql import SparkSession

from pyspark.sql import *



spark = SparkSession.builder.appName('table').config("spark.jars","C:\Installers\Drivers\postgresql-42.6.0.jar").getOrCreate()



properties = {

 "driver": "org.postgresql.Driver",
 "user": "postgres",
 "password": "Lahari@4d4"

}


url = "jdbc:postgresql://localhost:5432/Sales_db"



# reading data files


customers = spark.read.jdbc(url=url, table='customers', properties=properties)

items = spark.read.jdbc(url=url, table='items', properties=properties)

orders = spark.read.jdbc(url=url, table='orders', properties=properties)

order_details = spark.read.jdbc(url=url, table='order_details', properties=properties)

salesperson = spark.read.jdbc(url=url, table='salesperson', properties=properties)

ship_to = spark.read.jdbc(url=url, table='ship_to', properties=properties)



tables = [customers,items,orders,order_details,salesperson,ship_to]





for table in tables:
    print(table.show())
cust_path = "C:/parquets/cust.parquet"
items_path = "C:/parquets/items.parquet"
orders_path = "C:/parquets/orders.parquet"
order_details_path = "C:/parquets/order_details.parquet"
sp_path = "C:/parquets/salesperson.parquet"
ship_path = "C:/parquets/ship_to.parquet"



paths = [cust_path,items_path,orders_path,order_details_path,sp_path,ship_path]



for t,p in zip(tables,paths):
    t.write.parquet(p)

