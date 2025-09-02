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

%run "Extraction - Stage 1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "Loader - Stage 1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

%run "Transformer - Stage 1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### || Start of business logic pipelines initialization - 1st Logic ||

# CELL ********************

class FirstBusinessLogic:

    def __init__(self):
        pass
    
    def runner(self):
        # extract data from the lakehouse. This will use the extractor factory class and the extractor class from ETL. 
        Input_DFs = AirpodsAfterIphoneExtractor().extract()

        # Transform the extracted data based on the business logic provided.
        Filtered_DF = AirpodsAfterIphoneTransformer().transform(Input_DFs)

        # load the transformed DF to lakehouse as a delta table. 
        AirPodsAfterIphoneLoader(Filtered_DF).sink()
        
        Filtered_DF.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### || Start of business logic pipelines initialization - 2nd Logic ||

# CELL ********************

class SecondBusinessLogic:

    def __init__(self):
        pass
    
    def runner(self):
        # extract data from the lakehouse. This will use the extractor factory class and the extractor class from ETL. 
        Input_DFs = AirpodsAfterIphoneExtractor().extract()

        # Transform the extracted data based on the business logic provided.
        Filtered_DF = airpodsAndIphoneOnly().transform(Input_DFs)

        # load the transformed DF to lakehouse as a delta table.
        airpodsAndIphoneOnlyLoader(Filtered_DF).sink()
        
        Filtered_DF.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# this class will run the 2 pipeline data according to the need.

class WorkFlowRunner:

    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "FirstPipeline":
            return FirstBusinessLogic().runner()
        elif self.name == "SecondPipeline":
            return SecondBusinessLogic().runner()
        else:
            raise ValueError(f"Not Implemented for {self.name}")

# Initialize the pipeline names that you need to run
# name = "FirstPipeline" 
name = "SecondPipeline" 

# Call the class using object.
workFlowrunner = WorkFlowRunner(name).runner()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
