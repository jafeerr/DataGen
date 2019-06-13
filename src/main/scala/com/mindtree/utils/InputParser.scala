package com.mindtree.utils

import com.mindtree.entities.ColumnInfo
import com.mindtree.constants.Constants._
import com.mindtree.enumeration.YesOrNo
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

import scala.util.{Failure, Success, Try}

class InputParser(spark: SparkSession) {

  def readAndParseColumnInfo(inputPath: String): Array[ColumnInfo] = {

    val columnInfoDF =
      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(inputPath)
        .collect()
    columnInfoDF
      .map(parseColumnInfo)
      .filterNot(columnInfo =>
        columnInfo == None || columnInfo.get.columnName == null || columnInfo.get.columnName.isEmpty)
      .map(_.get)
  }

  private def parseColumnInfo(row: Row): Option[ColumnInfo] = {

    val columnInfo = Try {
      ColumnInfo(
        row.getInt(0),
        row.getString(1).trim,
        row.getString(2).trim,
        parseYesOrNo(row.getString(3)),
        parseYesOrNo(row.getString(4)),
        parseYesOrNo(row.getString(5)),
        parseYesOrNo(row.getString(6)),
        parseDataType(row.getString(7)),
        row.getString(8),
        row.getString(9),
        parseYesOrNo(row.getString(10)),
        row.getString(11),
        parseYesOrNo(row.getString(12)),
        parseYesOrNo(row.getString(13)),
        row.getString(14),
        resolveParentColumn(row),
        parseYesOrNo(row.getString(16)),
        parseYesOrNo(row.getString(17)),
        row.getString(18),
        parseYesOrNo(row.getString(19)),
        row.getString(20),
        parseYesOrNo(row.getString(21)),
        parseYesOrNo(row.getString(22)),
        row.getString(23),
        row.getString(24),
        parseYesOrNo(row.getString(25)),
        None,
        getInt(row, 26)
      )
    }
    columnInfo match {
      case Success(columnInfo) => Some(columnInfo)
      case Failure(exception)  => exception.printStackTrace(); None
    }
  }
  private def getInt(row: Row, index: Int) = {
    try {
      row.getInt(index)
    } catch {
      case _ => 10
    }

  }
  private def resolveParentColumn(row: Row) = {
    if (parseYesOrNo(row.getString(13)) && (row.getString(15) == null || row
          .getString(15)
          .isEmpty)) {
      row.getString(2).trim
    } else
      row.getString(15)
  }
  private def parseYesOrNo(input: String): Boolean = {
    if (input == null || input.isEmpty || NoValues.contains(input))
      false
    else
      true
  }

  private def parseDataType(input: String): DataType = {
    input match {
      case "int" | "smallint"        => IntegerType
      case "float"                   => FloatType
      case "double"                  => DoubleType
      case "timestamp" | "Timestamp" | "Time" | "time" => TimestampType
      case "long" | "bigint"         => LongType
      case "date"                    => DateType
      case "boolean"                 => BooleanType
      case _                         => StringType
    }
  }
  private def schema: StructType =
    StructType(
      Array(
        StructField("serielNo", IntegerType, nullable = false),
        StructField("fileName", StringType, nullable = false),
        StructField("columnName", StringType, nullable = false),
        StructField("isPrimaryKey", StringType, nullable = false),
        StructField("toGenerateDupData", StringType, nullable = false),
        StructField("isNotNull", StringType, nullable = false),
        StructField("toGenerateNullRows", StringType, nullable = false),
        StructField("dataType", StringType, nullable = false),
        StructField("length", StringType, nullable = false),
        StructField("decimal", StringType, nullable = false),
        StructField("toGenerateRowsViolatingDT", StringType, nullable = false),
        StructField("format", StringType, nullable = false),
        StructField("toGenerateRowsViolatingFormat",
                    StringType,
                    nullable = false),
        StructField("isForeignKey", StringType, nullable = false),
        StructField("parentFileName", StringType, nullable = false),
        StructField("parentColumnName", StringType, nullable = false),
        StructField("toGenerateRowsViolatingFK", StringType, nullable = false),
        StructField("isFromLOV", StringType, nullable = false),
        StructField("LOVFileName", StringType, nullable = false),
        StructField("toGenerateRowsViolatingLOV", StringType, nullable = false),
        StructField("defaultValue", StringType, nullable = false),
        StructField("toGenerateRowsViolatingDV", StringType, nullable = false),
        StructField("isShouldBeInRange", StringType, nullable = false),
        StructField("rangeStartValue", StringType, nullable = false),
        StructField("rangeEndValue", StringType, nullable = false),
        StructField("toGenerateRowsViolatingRange",
                    StringType,
                    nullable = false),
        StructField("noOfUniqueValues", IntegerType, nullable = false)
      )
    )

}
