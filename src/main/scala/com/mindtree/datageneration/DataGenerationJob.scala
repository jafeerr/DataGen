package com.mindtree.datageneration

import com.mindtree.entities.{ColumnInfo, Config, PrimaryKeyParams}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import com.mindtree.utils.RandomUtils._
import com.mindtree.utils.Common._
import com.mindtree.constants.Constants._
import org.apache.spark.sql.expressions.Window
import com.mindtree.constraints._
import scala.collection.mutable.ListBuffer

class DataGenerationJob(spark: SparkSession,
                        columnInfoList: Array[ColumnInfo],
                        noOfUniqueValues: Int = Integer.MAX_VALUE,
                        config: Config)
    extends Serializable
    with ForeignKeySupport {

  val corruptRecordStartId = config.noOfRows - ((config.noOfRows * (config.corruptRecordPercent)) / 100).toInt
  var totCombinations: Long = 0
  def generate() = {

    val columnInfoMap = columnInfoList
      .groupBy(_.fileName)
      .map(
        x =>
          (x._1,
           (x._2,
            x._2.foldLeft(false)((isChildTable, columnInfo) =>
              isChildTable || columnInfo.isForeignKey))))
      .toSeq
      .sortBy(_._2._2)

    for ((filename, (columnInfoArray, isChildTable)) <- columnInfoMap) {
      println("Process Started for table:"+filename)
      //Group by foreign keys
      val colInfoMap = columnInfoArray
        .groupBy(_.parentFileName)

      val foreignKeyMap = colInfoMap.-(null)

      val foreignKeyDF = processForeignKeys(foreignKeyMap)

      val nonFKcolInfoMap =
        colInfoMap.get(null).getOrElse(Array()).groupBy(_.isPrimaryKey)

      val primaryKeys =
        columnInfoArray
          .filter(_.isPrimaryKey)
          .map(x => col(x.columnName))

      val primaryWindowFn =
        Window.partitionBy(primaryKeys: _*).orderBy(primaryKeys: _*)

      val primaryKeyDF =
        processPrimaryKeys(spark,
                           nonFKcolInfoMap.get(true).getOrElse(Array()),
                           foreignKeyDF)

      val nonPrimaryKeyInfos = nonFKcolInfoMap.get(false).getOrElse(Array())
      val result = Array
        .tabulate(nonPrimaryKeyInfos.size)(index => index)
        .foldLeft(primaryKeyDF)((df, index) =>
          addField(df, index, nonPrimaryKeyInfos))
        .withColumn(RankCol, row_number() over primaryWindowFn)
        .filter(col(RankCol) === 1)
      val dropCols = result.columns
        .filter(_.endsWith(PKIndexSuffix))
        .toSeq ++ Seq(IdCol, RankCol, TempCol)

      val finalDF = result.drop(dropCols: _*)
      writeCSV(finalDF, config.outputPath, filename.trim)
      println(filename+" table generated")
    }
  }
  private def processPrimaryKeys(spark: SparkSession,
                                 columnInfoList: Array[ColumnInfo],
                                 dataFrame: DataFrame) = {
    if (columnInfoList.isEmpty)
      if (dataFrame.head(2).size > 1) dataFrame
      else spark.range(0, config.noOfRows + 1).toDF()
    else {
      println("Resolving Primary key columns")
      val columnInfoListWithParams = columnInfoList.map(addPrimaryKeyParams)

      totCombinations = columnInfoListWithParams
        .filterNot(_.primaryKeyParams == None)
        .foldLeft(1L)((sum, columnInfo) =>
          sum * columnInfo.primaryKeyParams.get.noOfValues)

      val temp =
        if (totCombinations == 1) (config.noOfRows + 1) else totCombinations

      val primaryKeyColInfoList =
        columnInfoListWithParams.map(updatePartitionIndex)
      val df = dataFrame.crossJoin(spark.range(0, temp).toDF())

      val dfWithIndex = primaryKeyColInfoList.foldLeft(df)(addPrimaryKeyIndex)

      val result = Array
        .tabulate(primaryKeyColInfoList.size)(index => index)
        .foldLeft(dfWithIndex)((df, index) =>
          addField(df, index, primaryKeyColInfoList))
        .withColumn(RandomNoCol, getRandomIntUDF(config.noOfRows + 1))
          .orderBy(RandomNoCol).limit(config.noOfRows)
          .drop(RandomNoCol)
      result

    }

  }

  private def processForeignKeys(
      foreignKeyMap: Map[String, Array[ColumnInfo]]) = {

    val df = spark.range(1, 2).toDF(TempCol)
    if (foreignKeyMap.isEmpty)
      df
    else {
      println("Resolving Foreign key columns")

      foreignKeyMap
        .foldLeft(df)(
          (df, map) =>
            df.crossJoin(
              getForeignKeyCombFromParent(spark,
                                          config.outputPath,
                                          map._1,
                                          map._2)))
    }

  }
  private def updatePartitionIndex(columnInfo: ColumnInfo): ColumnInfo = {

    val param =
      if (columnInfo.primaryKeyParams == None) None
      else {
        totCombinations = totCombinations / columnInfo.primaryKeyParams.get.noOfValues
        Some(
          columnInfo.primaryKeyParams.get
            .copy(partitionIndex = totCombinations))
      }

    columnInfo.copy(primaryKeyParams = param)
  }
  private def addPrimaryKeyIndex(dataFrame: DataFrame,
                                 columnInfo: ColumnInfo) = {
    val primaryKeyParam = columnInfo.primaryKeyParams
    if (primaryKeyParam == None)
      dataFrame
    else
      dataFrame.withColumn(columnInfo.columnName + PKIndexSuffix,
                           col(IdCol)
                             .divide(primaryKeyParam.get.partitionIndex)
                             .mod(primaryKeyParam.get.noOfValues)
                             .cast(IntegerType))
  }
  private def addPrimaryKeyParams(columnInfo: ColumnInfo) = {

    val primaryKeyParam = if (columnInfo.isForeignKey) {
      val values = getParentIdsFromCSV(spark,
                                       config.outputPath,
                                       columnInfo.parentFileName,
                                       columnInfo.parentColumnName)
      Some(PrimaryKeyParams(0, values.size, 0, values))
    } else if (columnInfo.isFromLOV) {
      val values =
        getListOfValues(spark, config.listOfValuePath, columnInfo.LOVFileName)
      Some(PrimaryKeyParams(0, values.size, 0, values))
    } else if (columnInfo.isShouldBeInRange) {
      Some(
        PrimaryKeyParams(
          0,
          columnInfo.rangeEndValue.toLong - columnInfo.rangeStartValue.toLong,
          0,
          Array[String]()))
    } else if (columnInfo.noOfUniqueValues > 0) {
      Some(
        PrimaryKeyParams(0,
                         columnInfo.noOfUniqueValues,
                         0,
                         Array.tabulate(columnInfo.noOfUniqueValues)(i =>
                           (i + 1).toString)))
    } else None
    columnInfo.copy(primaryKeyParams = primaryKeyParam)
  }
  private def addField(dataFrame: DataFrame,
                       colIndex: Int,
                       fields: Array[ColumnInfo]): DataFrame = {
    val columnInfo = fields(colIndex)
    dataFrame.withColumn(columnInfo.columnName,
                         getCellValue(columnInfo, colIndex, fields.size))
  }

  private def getCellValue(columnInfo: ColumnInfo,
                           colIndex: Int,
                           totalNoOfCols: Int) = {

    //val udfGetColumnPosition = udf(getColumnPosition _)

    val value = computeCellValue(columnInfo)

    /*when(
      col(IdCol).geq(lit(corruptRecordStartId)) and (udfGetColumnPosition(
        col(IdCol).minus(lit(corruptRecordStartId)),
        lit(colIndex),
        lit(totalNoOfCols)) === 1),
      lit(null)
    ).otherwise(value)*/
    value
  }

  private def computeCellValue(columnInfo: ColumnInfo) = {
    if (columnInfo.isForeignKey && ForeignKeyDTs.contains(columnInfo.dataType)) {
      val result =
        if (columnInfo.isPrimaryKey)
          getValueFromArrUDF(columnInfo.columnName,
                             columnInfo.primaryKeyParams.get.values)
        else
          getRandomValueUDF(
            getParentIdsFromCSV(spark,
                                config.outputPath,
                                columnInfo.parentFileName,
                                columnInfo.parentColumnName))
      result.cast(columnInfo.dataType)

    } else if (columnInfo.isFromLOV && LovDTs.contains(columnInfo.dataType)) {
      val result =
        if (columnInfo.isPrimaryKey)
          getValueFromArrUDF(columnInfo.columnName,
                             columnInfo.primaryKeyParams.get.values)
        else
          getRandomValueUDF(
            getListOfValues(spark,
                            config.listOfValuePath,
                            columnInfo.LOVFileName))

      result.cast(columnInfo.dataType)
    } else if (columnInfo.isShouldBeInRange && RangeDTs.contains(
                 columnInfo.dataType)) {
      val result =
        if (columnInfo.isPrimaryKey)
          getNoBWRangeUDF(columnInfo.columnName,
                          columnInfo.rangeStartValue.toInt,
                          columnInfo.rangeEndValue.toInt)
        else
          getValueFromRange(columnInfo)
      result

    } else if (columnInfo.isPrimaryKey) {
      columnInfo.dataType match {
        case IntegerType | LongType =>
          getValueFromArrUDF(columnInfo.columnName,
                             columnInfo.primaryKeyParams.get.values)

        case TimestampType | DateType => getRandomDateUDF(columnInfo)
        case _ =>
          concat(lit(columnInfo.columnName),
                 lit("_"),
                 getValueFromArrUDF(columnInfo.columnName,
                                    columnInfo.primaryKeyParams.get.values))

      }
    } else if (columnInfo.defaultValue != null) {
      lit(columnInfo.defaultValue)
    } else
      getRandomValue(columnInfo)
  }
  private def getValueFromRange(columnInfo: ColumnInfo) = {
    columnInfo.dataType match {
      case TimestampType | DateType =>
        getRandomDateBWRangeUDF(columnInfo.rangeStartValue,
                                columnInfo.rangeEndValue,
                                columnInfo.format)
      case IntegerType | LongType =>
        getRandomNoBWRangeUDF(columnInfo.rangeStartValue.toInt,
                              columnInfo.rangeEndValue.toInt)

      case DoubleType | FloatType =>
        round(
          getRandomNoBWRangeUDF(
            Math.ceil(columnInfo.rangeStartValue.toDouble).toInt,
            Math.floor(columnInfo.rangeEndValue.toDouble).toInt
          ).plus(getRandomDoubleUDF),
          3
        )
      case _ => lit(null).cast(columnInfo.dataType)
    }
  }
  private def getRandomValue(columnInfo: ColumnInfo) = {
    columnInfo.dataType match {
      case DataTypes.StringType =>
        concat(lit(columnInfo.columnName), col(IdCol))
      case DataTypes.TimestampType                    => getRandomDateUDF(columnInfo)
      case DataTypes.DateType                         => getRandomDateUDF(columnInfo)
      case DataTypes.FloatType | DataTypes.DoubleType => getRandomDoubleUDF
      case DataTypes.IntegerType | DataTypes.LongType =>
        getRandomIntUDF(noOfUniqueValues)
      case DataTypes.BooleanType => getRandomBoolean
      case _                     => lit(null).cast(columnInfo.dataType)
    }
  }
  private def getInvalidValue(columnInfo: ColumnInfo): Unit = {
    var invalidValues = new ListBuffer[String]()
    if (!columnInfo.isNotNull)
      invalidValues += null
    if (columnInfo.toGenerateRowsViolatingDT)
      invalidValues += columnInfo.columnName

  }
  private def getColumnPosition(id: Int, colIndex: Int, totalNoOfCols: Int) = {
    val formatStr = "%0" + totalNoOfCols + "d"
    val binaryStr = formatStr.format(id.toBinaryString.toInt)

    if (binaryStr.size < (colIndex + 1))
      0
    else
      binaryStr.charAt(colIndex).toString.toInt

  }

}
