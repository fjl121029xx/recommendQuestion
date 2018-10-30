package com.li.recommend

import java.text.DecimalFormat

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

case class parseRating(
                        val year: String,
                        val area: Int,
                        val firPoint: Int,
                        val secPoint: Int,
                        val thrPoint: Int
                      )

object ZtkQuestionRecord {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"
    val collection = "ztk_question"

    val conf = new SparkConf()
      .setAppName("ZtkQuestionRecord")
      .setMaster("local[8]")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.default.parallelism", "8")
      .set("spark.scheduler.mode", "FAIR")
    //      .set("spark.sql.autoBroadcastJoinThreshold", "1048576")


    val session = SparkSession.builder
      .config(conf)
      .config("spark.mongodb.input.uri", inputUrl)
      .config("spark.mongodb.input.collection", collection)
      .getOrCreate()
    val sc = session.sparkContext


    //    MongoSpark.load(session)

    import session.implicits._

    val df = session.read.format("com.mongodb.spark.sql.DefaultSource").options(
      Map("spark.mongodb.input.uri" -> inputUrl,
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "32",
        "spark.mongodb.keep_alive_ms" -> "360000000"
      )
    ).load


    val features = df.select("_id", "type", "year", "area", "mode", "subject", "difficult", "points").cache()

    val qid2features = features.mapPartitions(
      rs => {
        var x: Seq[Tuple2[Int, String]] = Seq()

        while (rs.hasNext) {
          val row = rs.next()

          val decimalFormatter = new DecimalFormat()
          decimalFormatter.setMaximumFractionDigits(3)
          val a: BigDecimal = 23

          var points: Seq[Int] = Seq()
          if (row.isNullAt(7) || row.getSeq[Int](7).length == 0) {
            points = points ++ Seq(0, 0, 0)
          } else if (row.getSeq[Int](7).length == 2) {
            points = 0 +: row.getSeq[Int](7)
          } else {
            points = points ++ row.getSeq[Int](7)
          }

          if (points.length != 3) {

            println(row)
          }

          var difficult = -1

          if (!row.isNullAt(6)) {
            difficult = row.getInt(6)
          }

          //特征值 _id type year area mode subject difficult points
          val t = new Tuple2(row.getInt(0),
            row.getInt(1) + "," + row.getInt(2) + "," + row.getInt(3) + "," + row.getInt(4) + "," + row.getInt(5) + "," + difficult + "," + points.mkString(","))

          x = t +: x
        }
        x.iterator
      }
    ).rdd


    val featuresMap = qid2features.collectAsMap()


    val trainingData = qid2features.map {
      t =>
        Vectors.dense(t._2.split(",").map(_.toDouble))
    }.cache()


    //特征值 _id type year area mode subject difficult points
    //    val trainingData = sc.textFile("hdfs://master/ztk_question_record/ztk_question_features/part-00000")
    //      .map {
    //        line =>
    //          Vectors.dense(line.split(",").slice(1, 10).map(_.toDouble))
    //      }.cache()


    val globalQid2Features = sc.broadcast(featuresMap)


    val testData = sc.textFile("hdfs://master/ztk_question_record/v_question_user_cache_*/*")
      .filter(
        _.split("_").length > 2
      )
      .flatMap(
        line => {

          var list: Seq[String] = Seq()

          val fields = line.split("_")
          var user_id = fields(0)
          var question_id_list = fields(2).split(",")

          for (x <- question_id_list) {


            list = (user_id + "," + globalQid2Features.value.get(x.toInt).get.toString) +: list
          }

          list.iterator
        }

      ).map {
      line =>
        (line.split(",")(0), Vectors.dense(line.split(",").slice(1, 10).map(_.toDouble)))
    }


    val numClusters = 350
    val numIterations = 40
    val runTimes = 3
    var clusterIndex: Int = 0
    var initializationMode = "k-means||"

    val clusters: KMeansModel = KMeans.train(trainingData, numClusters, numIterations, initializationMode, runTimes)
    val predictCluster = sc.broadcast(clusters)

    val cost = clusters.computeCost(trainingData)
    println(s"computeCost => $cost")

    val kpoint2Location = Map[Int, Vectors.type]()

    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    testData.map {
      line =>
        val user_id = line._1
        val baseItem = predictCluster.value.predict(line._2)
        ((user_id, baseItem), 1)
    }.reduceByKey(_ + _).map {
      line =>
        line._1._1 + "\t" + line._1._2 + "\t" + line._2
    }.saveAsTextFile("hdfs://master/ztk_question_record/ztk_question_rating")


    //    testData.collect().foreach(line => {
    //      val predictedClusterIndex: Int = clusters.predict(line._2)
    //
    //      println("The data " + line._1 + " belongs to cluster " +
    //        predictedClusterIndex)
    //    })

    sc.stop()
  }
}
