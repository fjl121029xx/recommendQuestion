package com.li.recommend

import com.mongodb.spark.config.ReadConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, Map}

object Point2Question {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"

    val conf = new SparkConf()
      .setAppName("AbilityAssessment")
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionKey", "_id")
      .set("spark.mongodb.input.partitionSizeMB", "5120")
      .set("spark.mongodb.input.samplesPerPartition", "5000000")
      .set("spark.debug.maxToStringFields", "100")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))

    import com.mongodb.spark.sql._
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    // ztk_question
    /**
      * 获得题到知识点的映射
      */
    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "maxBatchSize" -> "1000000",
          "keep_alive_ms" -> "500")
      )).toDF() // Uses the ReadConfig
    ztk_question.printSchema()
    ztk_question.createOrReplaceTempView("ztk_question")


    val point2question = sparkSession.sql("select _id,points,pointsName,subject from ztk_question")
      .rdd
      .filter {
        f =>
          !f.isNullAt(0) && !f.isNullAt(1) && f.getSeq[Double](1).nonEmpty && f.getSeq[Double](1).size > 2
      }
      .mapPartitions {
        ite =>
          val arr = new ArrayBuffer[(String, String)]()

          while (ite.hasNext) {
            val next = ite.next()

            val _id = next.get(0).asInstanceOf[Double].intValue()
            val points = next.get(1).asInstanceOf[Seq[Double]].map { f => f.toInt }.seq

            val subject = next.get(3).asInstanceOf[Double].intValue()

            arr += Tuple2(subject + "-" + points(2), _id.toString)
          }
          arr.iterator
      }
      .reduceByKey((a, b) => a.concat(",").concat(b))

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.68,192.168.100.70,192.168.100.72")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.rootdir", "/hbase")
    hbaseConf.set("hbase.client.retries.number", "3")
    hbaseConf.set("hbase.rpc.timeout", "2000")
    hbaseConf.set("hbase.client.operation.timeout", "30")
    hbaseConf.set("hbase.client.scanner.timeout.period", "100")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "ztk_point_question")
    val hbasePar = point2question.mapPartitions {
      ite =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()

        while (ite.hasNext) {
          val t = ite.next()

          val point_id = t._1.toString
          val questions = t._2


          val put = new Put(Bytes.toBytes(point_id)) //行健的值
          put.add(Bytes.toBytes("question_info"), Bytes.toBytes("questions"), Bytes.toBytes(questions))

          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }

    hbasePar.saveAsHadoopDataset(jobConf)
  }
}
