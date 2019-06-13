package com.mindtree.constraints

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mindtree.utils.Common._

trait ForeignKeySupport {

  def validateForeignKeys(spark:SparkSession, childDF: DataFrame,
                          parentDF: DataFrame,
                          childCols: Array[String],
                          parentColsOpt: Option[Array[String]]) = {

    joinDataFrames(childDF,
                   parentDF,
                   leftCols = childCols,
                   rightCols = parentColsOpt,
                   rightSelectCols = Some(Array()))
  }


}
