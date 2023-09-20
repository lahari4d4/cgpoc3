from configparser import ConfigParser
from pyspark.sql import SparkSession


def main():
    spark:SparkSession = SparkSession.builder.config("spark.jars","C:\installers\Drivers\postgresql-42.6.0.jar").appName("jdbc").master("local").getOrCreate()
    config = ConfigParser()
    config_path = "C:/Users/pelahari/PycharmProjects/pythonProject/File.Properties"

    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)

    # Read Parquet files into DataFrames
    cust_path = config.get('parquet', 'cust_path')
    items_path = config.get('parquet', 'items_path')
    details_path = config.get('parquet', 'details_path')
    orders_path = config.get('parquet', 'orders_path')
    sales_path = config.get('parquet', 'sales_path')
    ship_path = config.get('parquet', 'ship_path')

    cust_df = spark.read.parquet(cust_path)
    items_df = spark.read.parquet(items_path)
    details_df = spark.read.parquet(details_path)
    orders_df = spark.read.parquet(orders_path)
    sales_df = spark.read.parquet(sales_path)
    ship_df = spark.read.parquet(ship_path)

    # Step 6: Get the latest date from the 'created_date' column of a DataFrame (replace with your actual column name)
    latest_date = cust_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)
    latest_date = items_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)
    latest_date = details_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)
    latest_date = orders_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)
    latest_date = sales_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)
    latest_date = ship_df.selectExpr("max(created_date) as latest_date").collect()[0]["latest_date"]
    print("Latest date from cust_df:", latest_date)

     # Define the properties for connecting to the database
    db_properties = {
        "driver": "org.postgresql.Driver",
        "user": "postgres",
        "password": "Lahari@4d4",
        "url": "jdbc:postgresql://localhost:5432/Sales_db"

    }

    # Step 7: Create DataFrames from database tables
    # Replace 'table_name' with your actual table names
    db_cust_df = spark.read.jdbc(url=db_properties["url"], table="customers", properties=db_properties)
    db_items_df = spark.read.jdbc(url=db_properties["url"], table="items", properties=db_properties)
    db_details_df = spark.read.jdbc(url=db_properties["url"], table="order_details", properties=db_properties)
    db_orders_df = spark.read.jdbc(url=db_properties["url"], table="orders", properties=db_properties)
    db_sales_df = spark.read.jdbc(url=db_properties["url"], table="salesperson", properties=db_properties)
    db_ship_df = spark.read.jdbc(url=db_properties["url"], table="ship_to", properties=db_properties)

    # Step 8: Filter records in the database DataFrames using the latest date
    db_cust_filtered = db_cust_df.filter(db_cust_df["created_date"] > latest_date)
    db_items_filtered = db_items_df.filter(db_items_df["created_date"] > latest_date)
    db_details_filtered = db_details_df.filter(db_details_df["created_date"] > latest_date)
    db_orders_filtered = db_orders_df.filter(db_orders_df["created_date"] > latest_date)
    db_sales_filtered = db_sales_df.filter(db_sales_df["created_date"] > latest_date)
    db_ship_filtered = db_ship_df.filter(db_ship_df["created_date"] > latest_date)

    # Step 9: Write the filtered DataFrames to the same local path in append mode
    #cust_path = cust_path.replace("file://", "")  # Remove 'file://' prefix if present
    db_cust_filtered.write.mode("append").parquet("C:/userstory3/cust_path")
    db_items_filtered.write.mode("append").parquet("C:/userstory3/items_path")
    db_details_filtered.write.mode("append").parquet("C:/userstory3/details_path")
    db_orders_filtered.write.mode("append").parquet("C:/userstory3/orders_path")
    db_sales_filtered.write.mode("append").parquet("C:/userstory3/sales_path")
    db_ship_filtered.write.mode("append").parquet("C:/userstory3/ship_path")

    # Show filtered DataFrames (optional)
    db_cust_filtered.show()
    db_items_filtered.show()
    db_details_filtered.show()
    db_orders_filtered.show()
    db_sales_filtered.show()
    db_ship_filtered.show()

    db_cust_filtered.write.mode('append').jdbc(url=db_properties["url"], table="customers_filtered", properties=db_properties)
    db_items_filtered.write.mode('append').jdbc(url=db_properties["url"], table="items_filtered", properties=db_properties)
    db_details_filtered.write.mode('append').jdbc(url=db_properties["url"], table="details_filtered", properties=db_properties)
    db_orders_filtered.write.mode('append').jdbc(url=db_properties["url"], table="orders_filtered", properties=db_properties)
    db_sales_filtered.write.mode('append').jdbc(url=db_properties["url"], table="sales_filtered", properties=db_properties)
    db_ship_filtered.write.mode('append').jdbc(url=db_properties["url"], table="ship_filtered", properties=db_properties)
if __name__ == "__main__":
    main()
