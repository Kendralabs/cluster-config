def run(path=r"datasets/lbp_edge.csv"):
    """
    The default home directory is hdfs://user/taproot all the sample data sets are saved to
    hdfs://user/taproot/datasets when installing through the rpm
    you will need to copy the data sets to hdfs manually otherwise and adjust the data set location path accordingly
    :param path: data set hdfs path can be full and relative path
    """
    NAME = "lbp"
    
    import taprootanalytics as ta

    ta.connect()

    #csv schema definition
    schema = [("source", ta.int32),
              ("labels", ta.vector(5)),
              ("ignore", str),
              ("dest", ta.int32),
              ("weight", ta.float32)]
