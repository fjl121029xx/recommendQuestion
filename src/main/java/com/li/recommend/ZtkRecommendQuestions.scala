package com.li.recommend

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ZtkRecommendQuestions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//            .setMaster("local")
      .setAppName("RecommendQuestion")
      .set("spark.reducer.maxSizeInFlight", "128m")

    val sc = new SparkContext(conf)

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
            arr += Tuple2(line(0).toInt + "-" + line(1).toInt, line(1).split(",").length)
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
            arr += Tuple2(line(0).toInt + "-" + line(1).toInt, line(1).split(",").length)
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
            arr += Tuple2(line(0).toInt + "-" + line(1).toInt, line(1).split(",").length)
          }
          arr.iterator
      }
    println(wrong.count())
    //    finish.join(wrong).take(100).foreach(println)
    finish.fullOuterJoin(wrong).mapPartitions {
      ite =>
        val arr = new ArrayBuffer[(String, String, Int, Double)]()
        val coll = collect.value


        while (ite.hasNext) {
          val next = ite.next()
          val user2Point = next._1
          val fin = next._2._1.getOrElse(0)
          val wro = next._2._2.getOrElse(0)
          val col = coll.getOrElse(user2Point, 0)

          if (fin + wro + col != 0) {
            val isGrasp = wro * 1.0 / (fin + wro + col)
            if (isGrasp > 0.30) {
              arr += Tuple4(user2Point.split("-")(0), user2Point.split("-")(1), 1, isGrasp)
            } else {
              arr += Tuple4(user2Point.split("-")(0), user2Point.split("-")(1), 0, isGrasp)
            }

          } else {
            arr += Tuple4(user2Point.split("-")(0), user2Point.split("-")(1), 0, 0.00)
          }
        }

        arr.iterator
    }.foreach(println)

  }
}
