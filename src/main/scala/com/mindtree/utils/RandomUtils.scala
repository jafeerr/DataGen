package com.mindtree.utils

import java.util.Calendar

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.functions._
import com.mindtree.constants.Constants._
import com.mindtree.entities.ColumnInfo
import org.apache.spark.sql.Column

import scala.util.Random

object RandomUtils {
  def getRandomIntUDF(max: Int) = {
    val randomInt = udf(getNextInt _)
    randomInt(lit(max))
  }
  private def getNextInt(maxNo: Int) = {
    Random.nextInt(maxNo)
  }
  def getRandomDoubleUDF = {
    def getNextInt = {
      Random.nextDouble()
    }
    val randomDouble = udf(getNextInt _)
    randomDouble()
  }

  def getRandomValueUDF(input: Array[String]): Column = {

    def getRandomValue(input: String) = {
      val arr = input.split(",");
      arr(Random.nextInt(arr.size))
    }
    val randomValueUDF = udf(getRandomValue _)
    randomValueUDF(lit(input.mkString(",")))
  }
  def getValueFromArrUDF(columnName: String, input: Array[String]) = {
    def getRandomValue(input: String, id: Int) = {
      val arr = input.split(",");
      arr(id % arr.size)
    }
    val randomValueUDF = udf(getRandomValue _)
    randomValueUDF(lit(input.mkString(",")), col(columnName + PKIndexSuffix))
  }
  def getRandomDateBWRangeUDF(start: String, end: String, format: String) = {
    val resolvedFormat = { if (format == null) DefaultDateFormat else format }
    def RandomDateBWRange(start: String, end: String, format: String) = {

      val dateFormat = new java.text.SimpleDateFormat(format)
      val startDate = dateFormat.parse(start)
      val endDate = dateFormat.parse(end)
      val diffInSecs = (endDate.getTime - startDate.getTime) / 1000
      val randomNo = Random.nextInt(diffInSecs.toInt)
      dateFormat.format(DateUtils.addSeconds(startDate, randomNo))
    }
    val randomDate = udf(RandomDateBWRange _)

    to_date(randomDate(lit(start), lit(end), lit(resolvedFormat)),
            resolvedFormat)
  }
  def getRandomDateUDF(columnInfo: ColumnInfo) = {
    def randomDateGen = {
      val dateFormat = new java.text.SimpleDateFormat(DefaultDateFormat)
      val date = Calendar.getInstance().getTime
      val randomNo = getNextInt(RandomDateGenUpperLimit * DayToSecondsConvRate) - (RandomDateCuttOff * DayToSecondsConvRate)
      dateFormat.format(DateUtils.addSeconds(date, randomNo))
    }
    val randomDate = udf(randomDateGen _)
    date_format(randomDate(),columnInfo.format)
  }
//Random column
  def getRandomNoBWRangeUDF(start: Int, end: Int): Column = {

    def randomNo(start: Int, end: Int) = {
      Random.nextInt(end - start + 1) + start
    }
    val udfRandomNo = udf(randomNo _)
    udfRandomNo(lit(start), lit(end))
  }
  //Based on Id Column
  def getNoBWRangeUDF(columnName: String, start: Int, end: Int) = {
    def randomNo(start: Int, end: Int, index: Int) = {
      index + start
    }
    val udfRandomNo = udf(randomNo _)
    udfRandomNo(lit(start), lit(end), col(columnName + PKIndexSuffix))
  }

  def getRandomBoolean = {
    val randomBoolean = udf(Random.nextBoolean _)
    randomBoolean()
  }
}
