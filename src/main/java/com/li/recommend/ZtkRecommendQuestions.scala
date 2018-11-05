package com.li.recommend

import com.mongodb.spark.config.ReadConfig
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

case class ZtkRecommendQuestions(
                                  user_id: Int,
                                  question_point_id: String,
                                  isGrasp: Int,
                                  wrongRate: Double

                                )

object ZtkRecommendQuestions {

  def main(args: Array[String]): Unit = {


    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RecommendQuestion")
      .set("spark.reducer.maxSizeInFlight", "128m")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionKey", "_id")
      .set("spark.mongodb.input.partitionSizeMB", "5120")
      .set("spark.mongodb.input.samplesPerPartition", "5000000")
      .set("spark.debug.maxToStringFields", "100")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))

    import com.mongodb.spark.sql._

    val sparkSql = SparkSession.builder().config(conf).getOrCreate()
    import sparkSql.implicits._

    val sc = sparkSql.sparkContext

    val ztk_question = sparkSql.loadFromMongoDB(
      ReadConfig(
        mutable.Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "maxBatchSize" -> "1000000",
          "keep_alive_ms" -> "500")
      )).toDF() // Uses the ReadConfig
    ztk_question.printSchema()
    ztk_question.createOrReplaceTempView("ztk_question")

    val question2points = sc.broadcast(sparkSql.sql("select _id, points,year,area,subject from ztk_question")
      .rdd
      .filter {
        f =>
          !f.isNullAt(0) && !f.isNullAt(1) && f.getSeq[Double](1).nonEmpty && f.getSeq[Double](1).size > 2 && !f.isNullAt(2) && !f.isNullAt(3) && !f.isNullAt(4)
      }
      .mapPartitions {
        ite =>
          val arr = new ArrayBuffer[(Int, String)]()

          while (ite.hasNext) {
            val next = ite.next()
            val _id = next.get(0).asInstanceOf[Double].intValue()
            val points = next.get(1).asInstanceOf[Seq[Double]].map { f => f.toInt }.seq
            val year = next.get(0).asInstanceOf[Double].intValue()
            val area = next.get(0).asInstanceOf[Double].intValue()
            val subject = next.get(0).asInstanceOf[Double].intValue()
            arr += Tuple2(_id, subject + ":" + year + ":" + area + ":" + points.last)
          }
          arr.iterator
      }.collectAsMap()) // 得到知识点集合


    /**
      * 1196836
      */
    val collect = sc.broadcast(sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_collect/*")
      .coalesce(10)
      .mapPartitions {
        ite =>
          var arr = new ArrayBuffer[(String, Int)]()

          while (ite.hasNext) {
            val next = ite.next()

            val line = next.split("_")
            val user_id = line(0).toInt
            val question_id = line(2).split(",")

            val q2p = question2points.value

            question_id.map(f => (q2p.getOrElse(f.toInt, ""), 1)).groupBy(_._1).map {
              case (a: String, b: Array[(String, Int)]) => {

                var count = 0

                b.foreach(f => count = count + f._2)
                (a, count)
              }
            }.foreach {
              f =>
                arr += Tuple2(user_id + "-" + f._1, f._2)
            }

          }
          arr.iterator
      }.collectAsMap())
    /**
      * 28291890
      */
    val finish = sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_finish")
      .coalesce(10)
      .mapPartitions {
        ite =>
          var arr = new ArrayBuffer[(String, Int)]()

          while (ite.hasNext) {

            val next = ite.next()

            val line = next.split("_")
            val user_id = line(0).toInt
            val question_id = line(2).split(",")

            val q2p = question2points.value

            question_id.map(f => (q2p.getOrElse(f.toInt, ""), 1)).groupBy(_._1).map {
              case (a: String, b: Array[(String, Int)]) => {
                var count = 0
                b.foreach(f => count = count + f._2)
                (a, count)
              }
            }.foreach {
              f =>
                arr += Tuple2(user_id + "-" + f._1, f._2)
            }
          }
          arr.iterator
      }

    println(finish.count())
    /**
      * 30705910
      */
    val wrong = sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_wrong")
      .coalesce(10)
      .mapPartitions {
        ite =>
          var arr = new ArrayBuffer[(String, Int)]()

          while (ite.hasNext) {
            val next = ite.next()

            val line = next.split("_")
            val user_id = line(0).toInt
            val question_id = line(2).split(",")

            val q2p = question2points.value

            question_id.map(f => (q2p.getOrElse(f.toInt, ""), 1)).groupBy(_._1).map {
              case (a: String, b: Array[(String, Int)]) => {
                var count = 0
                b.foreach(f => count = count + f._2)
                (a, count)
              }
            }.foreach {
              f =>
                arr += Tuple2(user_id + "-" + f._1, f._2)
            }
          }
          arr.iterator
      }
    println(wrong.count())

    //    finish.join(wrong).take(100).foreach(println)
    val userWhetherGrasp = finish.fullOuterJoin(wrong)
      .mapPartitions {
        ite =>
          val arr = new ArrayBuffer[ZtkRecommendQuestions]()
          val coll = collect.value

          while (ite.hasNext) {
            val next = ite.next()
            val user2Point = next._1
            val fin = next._2._1.getOrElse(0)
            val wro = next._2._2.getOrElse(0)
            val col = coll.getOrElse(user2Point, 0)

            if (fin + wro + col != 0) {
              val isGrasp = wro * 1.0 / (fin + wro + col)

              val up = user2Point.split("-")
              if (up.length > 1) {

                val user = up(0) match {
                  case "" => -1
                  case _ => up(0).toInt
                }
                val point = up(1) match {
                  case "" => ""
                  case _ => up(1)
                }
                if (isGrasp > 0.40) {
                  arr += ZtkRecommendQuestions(user, point, 1, isGrasp)
                }
                //                else {
                //                  arr += ZtkRecommendQuestions(user, point, 0, isGrasp)
                //                }
              }

            }
          }
          arr.iterator
      }

    //    val userNotGrasp = userWhetherGrasp.filter(_.isGrasp == 1)
    //    val userIsGrasp = userWhetherGrasp.filter(_.isGrasp == 0)

    val user_question_point_isGrasp = userWhetherGrasp.groupBy(_.user_id)
      .mapPartitions {
        ite =>
          val pids = question2points.value
          val arr = new ArrayBuffer[(Int, Seq[(String, Int, Double)])]()

          while (ite.hasNext) {
            val next = ite.next()

            val user_id = next._1
            val point_id_ite = next._2.iterator
            val s = new ArrayBuffer[(String, Int, Double)]()
            while (point_id_ite.hasNext) {

              val n = point_id_ite.next()

              val question_point_id = n.question_point_id
              val isGrasp = n.isGrasp
              val wrongRate = n.wrongRate

              s += Tuple3(question_point_id, isGrasp, wrongRate)
              s.sortBy(_._3)
            }
            import scala.util.control._

            arr += Tuple2(user_id, s)
          }
          arr.iterator
      }
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
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "ztk_user_question_point_NotGrasp")
    val hbasePar = user_question_point_isGrasp.repartition(339).mapPartitions {
      ite =>

        var buffer = new ArrayBuffer[(ImmutableBytesWritable, Put)]()

        while (ite.hasNext) {
          val t = ite.next()

          val user_id = t._1.toString
          val isGraspInfo = t._2

          val put = new Put(Bytes.toBytes(user_id)) //行健的值

          isGraspInfo.foreach {
            f =>
              //question_point_id, isGrasp, wrongRate
              val question_point_id = f._1.toString
              val isGrasp = f._2
              val wrongRate = f._3
              put.add(Bytes.toBytes("question_point_info"), Bytes.toBytes(question_point_id), Bytes.toBytes(wrongRate.toString))
          }

          buffer += Tuple2(new ImmutableBytesWritable, put)
        }
        buffer.iterator
    }

    hbasePar.saveAsHadoopDataset(jobConf)
  }

}


