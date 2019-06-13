package com.mindtree.datageneration

import java.util.Calendar

import com.mindtree.constants.Constants._
import com.mindtree.entities.{ColumnInfo, Config}
import com.mindtree.utils
import com.mindtree.utils.RandomUtils.getNextInt
import com.mindtree.utils.{Common, ConfigUtils, InputParser, RandomUtils}
import org.apache.commons.lang.time.DateUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val config = ConfigUtils.getConfig(args.apply(0))

    val parser = new InputParser(spark)
    val columnInfoList = parser.readAndParseColumnInfo(config.inputPath)

    val obj =
      new DataGenerationJob(spark,
                            columnInfoList,
                            4,
                            config)
    obj.generate()

    /*val arr = Array(2, 2, 3)
    val totNo = arr.reduce(_ * _)
    for (i <- 0 to totNo*2) {
      var temp = totNo
      for (no <- arr) {
        temp = temp / no
        print((i / (temp)) % no)
      }
      println()
    }*/
  }
  def getRandomDateUDF() = {
    def randomDateGen = {
      val dateFormat = new java.text.SimpleDateFormat(DefaultDateFormat)
      val date = Calendar.getInstance().getTime
      val randomNo = Random.nextInt(RandomDateGenUpperLimit * DayToSecondsConvRate) - (RandomDateCuttOff * DayToSecondsConvRate)
      println(dateFormat.format(DateUtils.addSeconds(date, randomNo)))
      dateFormat.format(DateUtils.addSeconds(date, randomNo))
    }
    val randomDate = udf(randomDateGen _)
    date_format(randomDate(), DefaultDateFormat)
  }
}
