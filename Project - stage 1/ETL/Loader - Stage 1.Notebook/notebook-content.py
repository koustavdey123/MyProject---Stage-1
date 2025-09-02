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

%run "LLC - Factory Design (Loader) - Stage 1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Loader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):

        pass

class AirPodsAfterIphoneLoader(Loader):

    def sink(self):
        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF,
            path = "abfss://4accdfab-f99d-4cd4-9b18-9283d1bb67f0@onelake.dfs.fabric.microsoft.com/367de844-9133-4d1e-ab11-ee314697c0b8/Tables/airpods after iphone logic", 
            method = "overwrite"
        ).load_data_frame()

class airpodsAndIphoneOnlyLoader(Loader):

    def sink(self):
        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF,
            path = "abfss://4accdfab-f99d-4cd4-9b18-9283d1bb67f0@onelake.dfs.fabric.microsoft.com/367de844-9133-4d1e-ab11-ee314697c0b8/Tables/airpods and iphone only logic", 
            method = "overwrite"
        ).load_data_frame()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
