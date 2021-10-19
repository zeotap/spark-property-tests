package com.zeotap.utility.spark.example.helper

import com.zeotap.utility.spark.example.helper.ColumnConstants._
import com.zeotap.utility.spark.traits.DataOption

object UserDefinedColumns {
  def zuid() = ZUID

  def zuid(option: DataOption) = ZUID.copy(options = option)

  def zuid(option: DataOption, values: List[String]) = ZUID.copy(options = option, values = values)

  def age() = AGE

  def age(option: DataOption) = AGE.copy(options = option)

  def age(option: DataOption, values: List[String]) = AGE.copy(options = option, values = values)

  def gender() = GENDER

  def gender(option: DataOption) = GENDER.copy(options = option)

  def gender(option: DataOption, values: List[String]) = GENDER.copy(options = option, values = values)

  def appUsageAsText = APPUSAGE

  def appUsageAsText(option: DataOption) = APPUSAGE.copy(options = option)

  def appUsageAsText(option: DataOption, values: List[String]) = APPUSAGE.copy(options = option, values = values)

  def appCategory() = APPCATEGORY

  def appCategory(option: DataOption) = APPCATEGORY.copy(options = option)

  def appCategory(option: DataOption, values: List[String]) = APPCATEGORY.copy(options = option, values = values)

  def rawIAB() = RAW_IAB

  def rawIAB(option: DataOption) = RAW_IAB.copy(options = option)

  def rawIAB(option: DataOption, values: List[String]) = RAW_IAB.copy(options = option, values = values)

  def adid() = ADID

  def adid(option: DataOption) = ADID.copy(options = option)

  def adid(option: DataOption, values: List[String]) = ADID.copy(options = option, values = values)

  def deviceOS() = DEVICEOS

  def deviceOS(option: DataOption) = DEVICEOS.copy(options = option)

  def deviceOS(option: DataOption, values: List[String]) = DEVICEOS.copy(options = option, values = values)

  def countryCode() = COUNTRYCODE

  def countryCode(option: DataOption) = COUNTRYCODE.copy(options = option)

  def countryCode(option: DataOption, values: List[String]) = COUNTRYCODE.copy(options = option, values = values)

  def otr() = OTR

  def otr(option: DataOption) = OTR.copy(options = option)

  def otr(option: DataOption, values: List[String]) = OTR.copy(options = option, values = values)

  def bundleid() = BUNDLEID

  def bundleid(option: DataOption) = BUNDLEID.copy(options = option)

  def bundleid(option: DataOption, values: List[String]) = BUNDLEID.copy(options = option, values = values)

  def timestamp() = TIMESTAMP

  def timestamp(option: DataOption) = TIMESTAMP.copy(options = option)

  def timestamp(option: DataOption, values: List[String]) = TIMESTAMP.copy(options = option, values = values)

  def date() = DATE

  def date(option: DataOption) = DATE.copy(options = option)

  def date(option: DataOption, values: List[String]) = DATE.copy(options = option, values = values)
}
