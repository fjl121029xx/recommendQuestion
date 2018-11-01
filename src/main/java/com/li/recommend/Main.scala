package com.li.recommend

object Main {

  def main(args: Array[String]): Unit = {


    val collect = "265671,263127,82337,48317".split(",").length
    val finish = "104122,106012,259631,263036,264959,55400,55405,55431,55432,55441,55443,55444,55885,56094,56595,56712,56883,57360,57386,57565,57586,57595,57638,57775,57903,59354,59388,72874,84146".split(",").length
    val wrong = "584146,57386,57638,57775,106012,55405,55400,55444,55441,57360".split(",").length

    println(wrong * 1.0 / (0 + finish + wrong))
  }
}
