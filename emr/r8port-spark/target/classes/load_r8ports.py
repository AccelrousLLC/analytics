from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream


if __name__ == "__main__":

    sc = SparkContext(appName="R8portLoaderKinesis")
    ssc = StreamingContext(sc, 1)
    appName = "fluffy_bunny"
    streamName = "user_activity_development"
    endpointUrl = "kinesis.us-west-2.amazonaws.com"
    regionName = "us-west-2"

    # lines = KinesisUtils.createStream(
    #     ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    ssc.start()
    ssc.awaitTermination()