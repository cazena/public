package com.cazena.examples

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu._
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.{Map, Set}

object KafkaKuduSparkStreamingJob {
  def main(args: Array[String]) {

    val kafkaConsumerGroup = args(0)
    val kafkaBrokers  = args(1)
    val kuduMasters   = args(2)

    val kuduTableName = "traffic_conditions"
    val topicsSet     = Set("traffic")

    val sparkConf   = new SparkConf().setAppName("KafkaKuduSparkStreaming")
    val spark       = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc         = new StreamingContext(spark.sparkContext, Seconds(5))
    val kuduContext = new KuduContext(kuduMasters, spark.sparkContext)

    val kafkaParams     = Map[String, String]("bootstrap.servers"  -> kafkaBrokers,
      "group.id"           -> kafkaConsumerGroup,
      "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "security.protocol"  -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka",
      "sasl.mechanism" -> "GSSAPI")

    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }

    val kuduTableSchema = StructType(
        //        column name   type       nullable
        StructField("as_of_time", LongType , false) ::
        StructField("avg_num_veh" , DoubleType, true ) ::
        StructField("min_num_veh", IntegerType , true ) ::
        StructField("max_num_veh", IntegerType , true) ::
        StructField("first_meas_time", LongType, true ) ::
        StructField("last_meas_time", LongType , true ) :: Nil)

    val kuduPrimaryKey = Seq("as_of_time")

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.addHashPartitions(List("as_of_time").asJava, 4).setNumReplicas(3)

    // 5. Call create table API
    kuduContext.createTable(
      // Table name, schema, primary key and options
      kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)


    val dstream         = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val windowedStream  = dstream.map(rec => (rec.value.split(",")(0).toLong,rec.value.split(",")(1).toInt)).window(Seconds(60))

    windowedStream.foreachRDD { rdd =>
      import spark.implicits._

      val dataFrame = rdd.toDF("measurement_time","number_of_vehicles")
      dataFrame.createOrReplaceTempView("traffic")

      val resultsDataFrame = spark.sql("""SELECT UNIX_TIMESTAMP() * 1000 as_of_time,
	                                            ROUND(AVG(number_of_vehicles), 2) avg_num_veh,
						    MIN(number_of_vehicles) min_num_veh,
						    MAX(number_of_vehicles) max_num_veh,
		                                    MIN(measurement_time) first_meas_time,
						    MAX(measurement_time) last_meas_time
						FROM traffic""")
      /* NOTE: All 3 methods provided  are equivalent UPSERT operations on the Kudu table and
           are idempotent, so we can run all 3 in this example (although only 1 is necessary) */

      // Method 1: All kudu operations can be used with KuduContext (INSERT, INSERT IGNORE,
      //           UPSERT, UPDATE, DELETE)
      kuduContext.upsertRows(resultsDataFrame, kuduTableName)

      // Method 2: The DataFrames API provides the 'write' function (results in a Kudu UPSERT)
      val kuduOptions: Map[String, String] = Map("kudu.table"  -> kuduTableName,
        "kudu.master" -> kuduMasters)
      resultsDataFrame.write.options(kuduOptions).mode("append").kudu

      // Method 3: A SQL INSERT through SQLContext also results in a Kudu UPSERT
      resultsDataFrame.createOrReplaceTempView("traffic_results")
      spark.read.options(kuduOptions).kudu.createOrReplaceTempView(kuduTableName)
      spark.sql(s"INSERT INTO TABLE $kuduTableName SELECT * FROM traffic_results")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
