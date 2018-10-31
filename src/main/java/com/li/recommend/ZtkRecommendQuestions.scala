package com.li.recommend

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ZtkRecommendQuestions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//      .setMaster("local")
      .setAppName("RecommendQuestion")

    val sc = new SparkContext(conf)

    /**
      * 1196836
      */
    val collect = sc.broadcast(sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_collect")
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
    val finish = sc.broadcast(sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_finish")
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
      * 30705910
      */
    val isGrasp = sc.textFile("hdfs://huatu70/ztk_question_record/v_question_user_cache_wrong")
      .mapPartitions {
        ite =>
          var arr = new ArrayBuffer[(Int, (Int, Int))]()
          val cast_collect = collect.value
          val cast_finish = finish.value
          while (ite.hasNext) {
            val next = ite.next()

            val line = next.split("_")

            val userId = line(0).toInt
            val question_point_id = line(1).toInt
            val length = line(2).split(",").length

            val collect_point_length = cast_collect.getOrElse(userId + "-" + question_point_id, 0)
            val finish_point_length = cast_finish.getOrElse(userId + "-" + question_point_id, 0)


            val wrongRate = length * 1.0 / (length + collect_point_length + finish_point_length)
            if (wrongRate > 0.30) {
              (userId, question_point_id, 0, wrongRate)
            } else {
              (userId, question_point_id, 1, wrongRate)
            }
          }

          arr.iterator
      }

    isGrasp.saveAsTextFile("hdfs://huatu70/ztk_question_record/" + args(0))
  }
}
