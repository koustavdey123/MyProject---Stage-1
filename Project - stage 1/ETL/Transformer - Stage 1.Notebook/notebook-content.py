# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "367de844-9133-4d1e-ab11-ee314697c0b8",
# META       "default_lakehouse_name": "Delta_Tables_Storage",
# META       "default_lakehouse_workspace_id": "4accdfab-f99d-4cd4-9b18-9283d1bb67f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "367de844-9133-4d1e-ab11-ee314697c0b8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import lead, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col, collect_set, array_contains, size, broadcast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
class Transformation:
    def __init__(self):
        pass
    
    def transform(self, Input_Dfs):
        pass




# Customers who bought airpods just after iphone purchase. 

class AirpodsAfterIphoneTransformer(Transformation):
    def transform(self, Input_DFs):

        Transaction_InputDF = Input_DFs.get("Transaction_InputDF")
        
        WindowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        Transaction_TransformedDF = Transaction_InputDF.withColumn(
            "nextProductName", lead("product_name").over(WindowSpec)
        )

        FilteredDF = Transaction_TransformedDF.filter(
            (col("product_name") == "iPhone") & (col("nextProductName") == "AirPods")
        )

        print("Transaction DF After First Business Logic and filtered")

        Customer_InputDF = Input_DFs.get("Customer_InputDF")
        
        JoinedDF = FilteredDF.join(Customer_InputDF, on="customer_id", how="left")

        return JoinedDF.select(
            "customer_id", 
            "customer_name", 
            "location"
        )










# Customer who bought both iphone and airpods only. Macbook buyers should be excluded. 

class airpodsAndIphoneOnly(Transformation):
    def transform(self, Input_DFs):

        Transaction_InputDF = Input_DFs.get("Transaction_InputDF")

        groupedDF = Transaction_InputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))

        FilteredDF = groupedDF.filter(
            (array_contains(col("products"), "AirPods")) & 
            (array_contains(col("products"), "iPhone")) & 
            (size(col("products")) == 2))

        Customer_inputDF = Input_DFs.get("Customer_InputDF")

        print("Transaction DF After Second Business Logic and filtered")

        JoinedDF = FilteredDF.join(Customer_inputDF, on= "customer_id", how="inner")

        return JoinedDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
