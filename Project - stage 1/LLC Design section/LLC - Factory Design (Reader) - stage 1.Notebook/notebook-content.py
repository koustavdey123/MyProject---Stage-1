# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

class dataSource:

    def __init__(self, path):
        self.path = path
    
    def getDataFrame(self):

        raise ValueError("Not Implemented")


class CSVdataSource(dataSource):
    def getDataFrame(self):
        
        return (
            spark.read
            .format("csv")
            .option("header", True)
            .load(self.path)
        )

class ParquetdataSource(dataSource):
    def getDataFrame(self):
        
        return (
            spark.read
            .format("parquet")
            .load(self.path)
        )

class DeltadataSource(dataSource):
    def getDataFrame(self):
        
        return (
            spark.read
            .format("delta")
            .option("header", True)
            .load(self.path)
        )


def get_DataSource(DataType, FilePath):
    
    if DataType == "csv":
        return CSVdataSource(FilePath)
    elif DataType == "parquet":
        return ParquetdataSource(FilePath)
    elif DataType == "delta":
        return DeltadataSource(FilePath)
    else:
        raise ValueError(f"Not Implemented for data Type: {DataType}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
