package com.mindtree.entities

import com.mindtree.enumeration.YesOrNo.YesOrNo
import org.apache.spark.sql.types.DataType

case class ColumnInfo(serielNo: Int,
                      fileName: String,
                      columnName: String,
                      isPrimaryKey: Boolean,
                      toGenerateDupData: Boolean,
                      isNotNull: Boolean,
                      toGenerateNullRows: Boolean,
                      dataType: DataType,
                      length: String,
                      decimal: String,
                      toGenerateRowsViolatingDT: Boolean,
                      format: String,
                      toGenerateRowsViolatingFormat: Boolean,
                      isForeignKey: Boolean,
                      parentFileName: String,
                      parentColumnName: String,
                      toGenerateRowsViolatingFK: Boolean,
                      isFromLOV: Boolean,
                      LOVFileName: String,
                      toGenerateRowsViolatingLOV: Boolean,
                      defaultValue: String,
                      toGenerateRowsViolatingDV: Boolean,
                      isShouldBeInRange: Boolean,
                      rangeStartValue: String,
                      rangeEndValue: String,
                      toGenerateRowsViolatingRange: Boolean,
                      primaryKeyParams: Option[PrimaryKeyParams]=None,
                      noOfUniqueValues:Int)
case class PrimaryKeyParams(totalCombValues: Long,
                            noOfValues: Long,
                            partitionIndex: Long,
                            values:Array[String])
case class Config(inputPath: String,
                  outputPath: String,
                  listOfValuePath: String,
                  noOfRows: Int,
                  corruptRecordPercent: Int)
