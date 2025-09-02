# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run "Apple Analysis - Pipelines"

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
