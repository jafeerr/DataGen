package com.mindtree.constants

import org.apache.spark.sql.types._

object Constants {

  val IdCol = "id"
  val RankCol="rank"
  val TempCol="temp"

  val InputCSVDelimiter = ","
  val RandomDateGenUpperLimit = 365
  val RandomDateCuttOff = RandomDateGenUpperLimit / 2
  val DefaultDateFormat = "yyyy-MM-dd hh:mm:ss"
  val PathSeperator = "/"
  val TxtExtension = ".txt"
  val CSVExtension = ".csv"
  val TempCsvSuffix="_temp"
  val ForeignKeyDTs = Array(StringType, IntegerType, LongType)
  val LovDTs = Array(StringType)
  val RangeDTs =
    Array(TimestampType, DateType, IntegerType, LongType, DoubleType, LongType)

  //Config file
  val NoOfRowsKey="NoOfRows"
  val InputFilePathKey="InputFilePath"
  val OutputFilePathKey="OutputFolderPath"
  val ListOfValuesFilePathKey="ListOfValuesPath"
  val CorruptRecordPercentKey="InvalidRecordPercent"

  val PKIndexSuffix="_PK_index"
  val RandomNoCol="random_no"
  val YesValues=Array("Yes","YES","true","Y")
  val NoValues=Array("No","NO","false","N")

  val DayToSecondsConvRate=86400
}
