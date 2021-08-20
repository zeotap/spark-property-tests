package com.zeotap.utility.spark.example

import com.zeotap.utility.spark.generator.RandomDataGenerator
import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types

object ColumnConstants {

  final val ZUID = types.DataColumn("zuid", DString, AlwaysPresent, RandomDataGenerator.UUID(20))

  final val AGE = types.DataColumn("age", DInteger, AlwaysPresent, RandomDataGenerator.age(20, 12, 100))

  final val GENDER = types.DataColumn("gender", DString, AlwaysPresent, List("Male", "Female"))

  final val APPUSAGE = types.DataColumn("appusage", DString, AlwaysPresent, List("[[com.picsart.studio, android, BRA, 2021-03-24]]",
    "[[com.vidfake.scarymo, android, BRA, 2021-03-24]]", "[[341232718, ios, BRA, 2021-03-27], [997362197, ios, BRA, 2021-03-24]]"))

  final val APPCATEGORY = types.DataColumn("appcategory", DString, AlwaysPresent, RandomDataGenerator.appCategory(20))

  final val RAW_IAB = types.DataColumn("rawIAB", DString, AlwaysPresent, RandomDataGenerator.rawIAB(20))

  final val ADID = types.DataColumn("adid", DString, AlwaysPresent, RandomDataGenerator.UUID(20))

  final val DEVICEOS = types.DataColumn("deviceos", DString, AlwaysPresent, List("iOS", "Android"))

  final val COUNTRYCODE = types.DataColumn("countrycode", DString, AlwaysPresent, RandomDataGenerator.country(20))

  final val OTR = types.DataColumn("otr", DDouble, AlwaysPresent, RandomDataGenerator.OTR(20))

  final val BUNDLEID = types.DataColumn("bundleid", DString, AlwaysPresent, RandomDataGenerator.bundleid(20))

  final val TIMESTAMP = types.DataColumn("timestamp", DLong, AlwaysPresent, RandomDataGenerator.timestamp(20))

  final val DATE = types.DataColumn("date", DString, AlwaysPresent, RandomDataGenerator.date(20))

}
