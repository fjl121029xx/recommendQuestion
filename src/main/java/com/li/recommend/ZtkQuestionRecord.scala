package com.li.recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object ZtkQuestionRecord {
//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ZtkQuestionRecord")
      //    .setMaster("local")
      .set("spark.sql.crossJoin.enabled", "true")

    val session = SparkSession.builder.config(conf).getOrCreate()

    var options: Map[String, String] = Map(
      "url" -> "jdbc:mysql://192.168.100.18/vhuatu",
      "dbtable" -> "v_question_user_cache_collect",
      "user" -> "vhuatu",
      "password" -> "vhuatu_2013"
    )
    val userQuestionCollect = session.read.format("jdbc").options(options).load
    userQuestionCollect.createGlobalTempView("v_question_user_cache_collect")

    options -= ("dbtable")
    options += ("dbtable" -> "v_question_user_cache_collect")
    val userQuestionWrong = session.read.format("jdbc").options(options).load
    userQuestionWrong.createGlobalTempView("v_question_user_cache_wrong")


    import session.implicits._
    val collectDf = session.sql("SELECT user_id,question_point_id,question_id FROM global_temp.v_question_user_cache_collect ")
    val collect = collectDf.map {
      r =>

        ((r.getInt(0), r.getInt(1)), r.getString(2).trim.split(",").length)
    }.rdd

    val wrongDf = session.sql("SELECT user_id,question_point_id,question_id FROM global_temp.v_question_user_cache_wrong")
    val wrong = wrongDf.map {
      r =>

        ((r.getInt(0), r.getInt(1)), r.getString(2).trim.split(",").length * 1.5)
    }.rdd


    var ranting = wrong.join(collect)

    ranting.map({
      r =>
        (r._1._1 + "," + r._1._2 + "," + r._2._1 + r._2._2)
    }).repartition(1).saveAsTextFile("hdfs://master/ztk_question_record/record01/")
  }
}
