package com.zeotap.utility.spark.example.helper

import com.zeotap.utility.spark.example.generator.RandomDataGenerator
import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types.DataColumn

object ColumnConstants {
  final val DefaultCollectionSize = 120
  final val JavaNull = null

  final val ZUID = DataColumn("zuid", DString, AlwaysPresent, RandomDataGenerator.UUID(20))

  final val AGE = DataColumn("age", DInteger, AlwaysPresent, RandomDataGenerator.age(20, 12, 100))

  final val GENDER = DataColumn("gender", DString, AlwaysPresent, List("Male", "Female"))

  final val APPUSAGE = DataColumn("appusage", DString, AlwaysPresent, List("[[com.picsart.studio, android, BRA, 2021-03-24]]",
    "[[com.vidfake.scarymo, android, BRA, 2021-03-24]]", "[[341232718, ios, BRA, 2021-03-27], [997362197, ios, BRA, 2021-03-24]]"))

  final val APPCATEGORY = DataColumn("appcategory", DString, AlwaysPresent, RandomDataGenerator.appCategory(20))

  final val RAW_IAB = DataColumn("rawIAB", DString, AlwaysPresent, RandomDataGenerator.rawIAB(20))

  final val ADID = DataColumn("adid", DString, AlwaysPresent, RandomDataGenerator.UUID(20))

  final val DEVICEOS = DataColumn("deviceos", DString, AlwaysPresent, List("iOS", "Android"))

  final val COUNTRYCODE = DataColumn("countrycode", DString, AlwaysPresent, RandomDataGenerator.country(20))

  final val OTR = DataColumn("otr", DDouble, AlwaysPresent, RandomDataGenerator.OTR(20))

  final val BUNDLEID = DataColumn("bundleid", DString, AlwaysPresent, RandomDataGenerator.bundleid(20))

  final val TIMESTAMP = DataColumn("timestamp", DLong, AlwaysPresent, RandomDataGenerator.timestamp(20))

  final val DATE = DataColumn("date", DString, AlwaysPresent, RandomDataGenerator.date(20))

  final val ID_TYPE = DataColumn("id_type", DString, AlwaysPresent, RandomDataGenerator.idType(20))
}
