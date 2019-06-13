package com.mindtree.utils

import java.io.File

import com.typesafe.config.ConfigFactory
import com.mindtree.constants.Constants._
import com.mindtree.entities.Config
object ConfigUtils {
  def getConfig(configFilePath: String) = {

    val config = ConfigFactory.parseFile(new File(configFilePath))
    Config(
      config.getString(InputFilePathKey),
      config.getString(OutputFilePathKey),
      config.getString(ListOfValuesFilePathKey),
      config.getInt(NoOfRowsKey),
      config.getInt(CorruptRecordPercentKey)
    )
  }
}
