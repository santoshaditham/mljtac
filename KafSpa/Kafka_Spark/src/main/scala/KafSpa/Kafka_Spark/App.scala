package KafSpa.Kafka_Spark

/**
 * @author ${user.name}
 */

import kafka.serializer.{ DefaultDecoder, StringDecoder }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.{ IntWritable, Text }
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{ HasOffsetRanges, KafkaUtils }
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
//import org.apache.phoenix.spark._


object KafSpa {
 
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <broker-list> <zk-list> <topic>")
      System.exit(1)
    }

    val Array(broker, zk, topic) = args
    
    /* All configs*/

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local[2]","Spark-from-Kafka-to-Hbase",sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val kafkaConf = Map("metadata.broker.list" -> broker,
      "auto.offset.reset" -> "smallest",
      "zookeeper.connect" -> zk,
      "group.id" -> "spark-streaming-from-kafka",
      "zookeeper.connection.timeout.ms" -> "2500")

    
    /*Direct Stream from Kafka to Spark*/

    val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set(topic)).map(_._2)

    /* Getting Kafka offsets from RDDs 
    lines.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(println(_))
    }*/

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(10), 2)

    wordCounts.print()
   

    /* Insert from Spark streaming into Hbase*/    
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.mapred.outputtable", "test_table")
    hbaseConf.set("hbase.mapreduce.scan.column.family", "word")
    hbaseConf.set("hbase.zookeeper.property.maxClientCnxns", "1000")

    wordCounts.saveAsNewAPIHadoopFiles(
      "_tmp",
      "_result",
      classOf[Text],
      classOf[IntWritable],
      classOf[TableOutputFormat[Text]],
      hbaseConf)
    
    /*Insert into Hbase via Phoenix
    val dataSet = List(("Telugu", 1), ("Tamil", 2), ("Hindi", 3))
    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "test_table",
        Seq("COL1","COL2"),
        zkUrl = Some("localhost:2181")
      )*/
      
    ssc.checkpoint("./checkpoints")  
    //ssc.checkpoint("hdfs://checkpoints") 
    ssc.start()
    ssc.awaitTermination()

  }

}

