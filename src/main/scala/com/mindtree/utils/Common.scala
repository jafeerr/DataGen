package com.mindtree.utils

import java.io.File

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import com.mindtree.constants.Constants._
import com.mindtree.entities.ColumnInfo
import org.apache.spark.sql.functions._
object Common {
  def getListOfValues(spark: SparkSession,
                      path: String,
                      fileName: String,
                      extension: String = "") = {
    val rdd =
      spark.sparkContext.textFile(path + PathSeperator + fileName + extension)
    val result = rdd.flatMap(x => x.split(",")).collect()
    result.foreach(println)
    result
  }
  def getParentIdsFromCSV(spark: SparkSession,
                          path: String,
                          parentFileName: String,
                          columnName: String,
                          extension: String = "") = {
    val df = spark.read
      .option("header", "true")
      .csv(path + PathSeperator + parentFileName + extension)

    df.select(columnName)
      .distinct
      .collect
      .map(row => row.getString(0))
  }
  def getForeignKeyCombFromParent(spark: SparkSession,
                                  path: String,
                                  parentFileName: String,
                                  foreignCols: Array[ColumnInfo],
                                  extension: String = "") = {
    val df = spark.read
      .option("header", "true")
      .csv(path + PathSeperator + parentFileName + extension)

    val foreignKeys = foreignCols.map(x => (x.columnName, x.parentColumnName))

    val result = df.select(foreignKeys.map(x => col(x._2)): _*).distinct
    foreignKeys.foldLeft(result)((result, foreignKeys) =>
      result.withColumnRenamed(foreignKeys._2, foreignKeys._1))
  }
  def readCSV(spark: SparkSession,
              path: String,
              fileName: String,
              extension: String = CSVExtension) = {
    spark.read
      .option("header", "true")
      .csv(path + PathSeperator + fileName + extension)
  }
  def writeCSV(dataframe: DataFrame,
               outputPath: String,
               outputFileName: String) = {
    println(dataframe.count())
    val csvPath = outputPath + PathSeperator + outputFileName
    dataframe
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(csvPath + TempCsvSuffix)

    deleteCRCFiles(csvPath)
  }
  def deleteCRCFiles(filePath: String) = {
    val file = new File(filePath + TempCsvSuffix)
    val fileList = file.list
    for (i <- 0 to fileList.length - 1) {
      val myFile = new File(file, fileList(i))
      if (fileList(i).endsWith(".crc") || fileList(i).contains("_SUCCESS")) {
        myFile.delete()
      } else {
        myFile.renameTo(new File(filePath + CSVExtension))
      }
    }
    file.delete()

  }
  def joinDataFrames(leftDF: DataFrame,
                     rightDF: DataFrame,
                     joinType: String = "inner",
                     leftCols: Array[String],
                     rightCols: Option[Array[String]] = None,
                     leftSelectCols: Option[Array[String]] = None,
                     rightSelectCols: Option[Array[String]] = None,
                     dropCols: Option[Array[String]] = None): DataFrame = {
    val leftDFTemp = leftDF.toDF(leftDF.columns: _*)
    val rightDFTemp = rightDF.toDF(rightDF.columns: _*)

    val rightColumns =
      if (rightCols == None || rightCols.get.isEmpty) leftCols
      else rightCols.get
    var cond: Column = Array
      .tabulate(leftCols.size)(index => index)
      .map(i => col("left." + leftCols(i)) === col("right." + rightColumns(i)))
      .reduce((cond1, cond2) => cond1 && cond2)

    val joinedDF = leftDFTemp
      .as("left")
      .join(rightDFTemp.as("right"), cond, joinType)
      .withColumn("temp", lit(0))

    val left =
      if (leftSelectCols == None) leftDFTemp.columns.map(x => "left." + x)
      else if (leftSelectCols.get.isEmpty) Array[String]()
      else
        leftSelectCols.get.map(x => "left." + x)
    val right =
      if (rightSelectCols == None) rightDFTemp.columns.map(x => "right." + x)
      else if (rightSelectCols.get.isEmpty)
        Array[String]()
      else
        rightSelectCols.get
          .map(x => "right." + x)
    val result =
      if (right.isEmpty)
        joinedDF.select("temp", left: _*)
      else if (left.isEmpty)
        joinedDF.select("temp", right: _*)
      else
        joinedDF.select("temp", (left ++ right): _*)

    if (dropCols == None)
      result.drop("temp")
    else
      result.drop("temp").drop(dropCols.get: _*)
  }
}
