from pyspark.storagelevel import StorageLevel
from pyspark.serializers import NoOpSerializer

__all__ = ['RabbitMQUtils', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8')


class RabbitMQUtils(object):

    @staticmethod
    def createStream(ssc, host, exchange, routing_key, queue,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_2,
                     decoder=utf8_decoder):

        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
        try:
            # Use RabbitMQUtilsPythonHelper to access Scala's RabbitMQ library
            helper = ssc._jvm.org.apache.spark.streaming.rabbitmq.RabbitMQUtilsPythonHelper()
        except TypeError as e:
            if str(e) == "'JavaPackage' object is not callable":
                RabbitMQUtils._printErrorMsg(ssc.sparkContext)
            raise
        jstream = helper.createStream(ssc._jssc, host, exchange, routing_key, queue, jlevel)
        stream = DStream(jstream, ssc, NoOpSerializer())
        return stream.map(lambda v: decoder(v))

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________
  Spark Streaming's RabbitMQ libraries not found in class path. %s / %s
________________________________________________________________________________________________
""" % (sc.version, sc.version))
