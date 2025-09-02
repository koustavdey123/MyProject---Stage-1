# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

class DataSink:
    """
    Abstract class 
    """

    def __init__(self, df, path, method):
        self.df = df
        self.path = path
        self.method = method


    def load_data_frame(self):
        """
        Abstract method, Function will be defined in sub classes
        """

        raise ValueError("Not Implemented")


class LoadToDeltaTable(DataSink):

    def load_data_frame(self):        
        
        self.df.write.format("delta").mode(self.method).save(self.path)

def get_sink_source(sink_type, df, path, method):
    if sink_type == "delta":
        return LoadToDeltaTable(df, path, method)
    else:
        return ValueError(f"Not implemented for sink type: {sink_type}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
