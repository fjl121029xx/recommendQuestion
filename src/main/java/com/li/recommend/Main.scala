package com.li.recommend

object Main {

  def main(args: Array[String]): Unit = {


    val collect = "265671,263127,82337,48317".split(",").length
    val finish = "42281".split(",").length
    val wrong = "40787,236604,40683,37837,39890".split(",").length

    println(wrong * 1.0 / (0 + finish + wrong))
  }
}
