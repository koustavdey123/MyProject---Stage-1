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

%run "./LLC - Factory Design (Reader) - stage 1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Extractor:

    def __init__(self):
        pass
    
    def extract(self):
        pass


class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        Transaction_InputDF = get_DataSource(
            DataType = "delta",
            FilePath = "abfss://4accdfab-f99d-4cd4-9b18-9283d1bb67f0@onelake.dfs.fabric.microsoft.com/367de844-9133-4d1e-ab11-ee314697c0b8/Tables/transaction_data_delta"
        ).getDataFrame()

        Customer_InputDF = get_DataSource(
            DataType="delta",
            FilePath="abfss://4accdfab-f99d-4cd4-9b18-9283d1bb67f0@onelake.dfs.fabric.microsoft.com/367de844-9133-4d1e-ab11-ee314697c0b8/Tables/customer_data_Delta"
        ).getDataFrame()
        

        #return the dataframes as Dictionary
        Input_DFs = {
            "Transaction_InputDF" : Transaction_InputDF,
            "Customer_InputDF" : Customer_InputDF
        }

        return Input_DFs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
