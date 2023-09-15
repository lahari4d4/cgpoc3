
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from configparser import ConfigParser


def main():
#Create separate menthod for sparksession
    spark = SparkSession.builder.appName('table').config("spark.jars","C:\Installers\Drivers\postgresql-42.6.0.jar").getOrCreate()

    properties = {

    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "Lahari@4d4"

}
    url = "jdbc:postgresql://localhost:5432/Sales_db"

# Using sparksession read the parquet file(us1)
# Read the Parquet file into a DataFrame
    cust_df = "C:/parquets/cust.parquet/part-00000-470d69c9-378d-4921-9f07-54eb161ce40d-c000.snappy.parquet"
    df1 = spark.read.parquet(cust_df)
    #df1.show()
    sales_df = "C:/parquets/salesperson.parquet/part-00000-ba4ec6d1-8b0b-483c-b71a-303ff94f59c7-c000.snappy.parquet"
    df2 = spark.read.parquet(sales_df)
    #df2.show()
    order_df = "C:/parquets/orders.parquet/part-00000-3957da02-4db0-4ec6-b822-8d898a360b64-c000.snappy.parquet"
    df3 = spark.read.parquet(order_df)
    #df3.show()
    items_df = "C:/parquets/items.parquet/part-00000-1bfb71f0-42c2-4617-9580-eaaffefd10a1-c000.snappy.parquet"
    df4 = spark.read.parquet(items_df)
    #df4.show()
    order_details_df = "C:/parquets/order_details.parquet/part-00000-f0e3c6f7-0e71-404b-a420-f4c5ca9dd63c-c000.snappy.parquet"
    df5 = spark.read.parquet(order_details_df)
    #df5.show()
    ship_to_df = "C:/parquets/ship_to.parquet/part-00000-42c4e36c-7af8-408c-88da-49ed8bfdae7c-c000.snappy.parquet"
    df6 = spark.read.parquet(ship_to_df)
    #df6.show()
 # Create temporary view using df

    df1.createOrReplaceTempView("cust_view")
    df2.createOrReplaceTempView("sales_view")
    df3.createOrReplaceTempView("order_view")
    df4.createOrReplaceTempView("items_view")
    df5.createOrReplaceTempView("order_details_view")
    df6.createOrReplaceTempView("ship_to_view")
    data1 = '''select c.cust_id,c.cust_name, count(od.ORDER_ID)  as total from cust_view c
    left join order_view rd on (c.CUST_ID= rd.cust_id )
    left join order_details_view od on ( rd.ORDER_ID = od. ORDER_ID)
    left join items_view i on (od.ITEM_ID = i.ITEM_ID)
    group by c.cust_id,c.cust_name'''
    data2 = '''select cust_id , cust_name,sum( ITEM_QUANTITY*DETAIL_UNIT_PRICE) total_order from (
    select c.cust_id,c.cust_name, od.ITEM_ID, od.ITEM_QUANTITY, od.DETAIL_UNIT_PRICE  from cust_view c  
    left join order_view rd on (c.CUST_ID= rd.cust_id )
    left join order_details_view od on ( rd.ORDER_ID = od. ORDER_ID)
    left join items_view i on (od.ITEM_ID = i.ITEM_ID) ) p
    group by cust_id , cust_name'''
    data3 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(ITEM_QUANTITY) as sums from items_view i
    left join order_details_view od on (od.ITEM_ID = i.ITEM_ID)
    group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sums  desc'''
    data4 = '''select i.CATEGORY , sum(od.ITEM_QUANTITY)  as sums from items_view i
    left join order_details_view od on (od.ITEM_ID = i.ITEM_ID)
    group by i.CATEGORY  order by sums desc'''
    data5 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as sums  from items_view i
    left join order_details_view od on (od.ITEM_ID = i.ITEM_ID)
    group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sums desc'''
    data6 = '''select i.CATEGORY , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as sums from items_view i
    left join order_details_view od on (od.ITEM_ID = i.ITEM_ID)
    group by i.CATEGORY  order by sums desc'''
    data8 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION from items_view i where 
    i.ITEM_ID not in (select item_id from order_details_view od )'''
    data9 = '''select c.CUST_ID, c.cust_name from cust_view c where 
    c.CUST_ID not in (select s.CUST_ID  from ship_to_view s)'''
    data10 = '''select c.CUST_ID, c.cust_name from cust_view c
    join ship_to_view s  on (s.cust_id =c.cust_id)  where length(s.POSTAL_CODE) !=6  '''
 #sparksession.sql return dataframe , add an extra column current_date
#partitioned the data in local path so use partitionedBy method as datewise
    df1 = spark.sql(data1).withColumn('Currentdate', current_date())
    mode = "overwrite"
    df1.write.jdbc(url=url, table='cust_output', mode=mode, properties=properties)
    df1.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/two')
    df1.show()
    df2 = spark.sql(data2).withColumn('Currentdate', current_date())
    df2.write.jdbc(url=url, table='sales_output', mode=mode, properties=properties)
    df2.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/three')
    df2.show()
    df3 = spark.sql(data3).withColumn('Currentdate', current_date())
    df3.write.jdbc(url=url, table='cust_output2', mode=mode, properties=properties)
    df3.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/four')
    df3.show()
    df4 = spark.sql(data4).withColumn('Currentdate', current_date())
    df4.write.jdbc(url=url, table='cust_output3', mode=mode, properties=properties)
    df4.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/five')
    df4.show()
    df5 = spark.sql(data5).withColumn('Currentdate', current_date())
    df5.write.jdbc(url=url, table='cust_output4', mode=mode, properties=properties)
    df5.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/six')
    df5.show()
    df6 = spark.sql(data6).withColumn('Currentdate', current_date())
    df6.write.jdbc(url=url, table='cust_output5', mode=mode, properties=properties)
    df6.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/seven')
    df6.show()
    df8 = spark.sql(data8).withColumn('Currentdate', current_date())
    df8.write.jdbc(url=url, table='cust_output6', mode=mode, properties=properties)
    df8.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/eight')
    df8.show()
    df9 = spark.sql(data9).withColumn('Currentdate', current_date())
    df9.write.jdbc(url=url, table='cust_output8', mode=mode, properties=properties)
    df9.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/nine')
    df9.show()
    df10 = spark.sql(data10).withColumn('Currentdate', current_date())
    df10.write.jdbc(url=url, table='cust_output9', mode=mode, properties=properties)
    df10.write.partitionBy('Currentdate') .mode("overwrite").parquet('C:/Users/temp_outs/ten')
    df10.show()


if __name__ == '__main__':
    main()


