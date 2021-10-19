package com.zeotap.utility.spark.ops

import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types._
import org.apache.spark.sql.types
import org.apache.spark.sql.types._


object DataColumnOps {

  implicit class DataColumnUtils(dc: DataColumn) {

    def asString = dc.copy(dataType = DString)

    def asInt = dc.copy(dataType = DInteger)

    def asLong = dc.copy(dataType = DLong)

    def asDouble = dc.copy(dataType = DDouble)

    def asBoolean = dc.copy(dataType = DBoolean)

    def withNull = dc.copy(values = null :: dc.values)

    /* *
    * Using withJunk for the below datatypes you can expect these junk values in your DF
    * DString => "junkValue", "null", "", " "
    * DInteger => 2147483647, -2147483648
    * DLong => 9223372036854775807, -9223372036854775808
    * DDouble => 1.7976931348623157E308, -1.7976931348623157E308
    * DBoolean => No junk values will be there for boolean type
    */

    def withJunk = dc.dataType match {
      case DString => dc.copy(values = List("junkValue", "null", "", " ") ::: dc.values)
      case DInteger => dc.copy(values = List(Integer.MAX_VALUE, Integer.MIN_VALUE).map(x => x.toString) ::: dc.values)
      case DLong => dc.copy(values = List(Long.MaxValue, Long.MinValue).map(x => x.toString) ::: dc.values)
      case DDouble => dc.copy(values = List(Double.MaxValue, Double.MinValue).map(x => x.toString) ::: dc.values)
      case DBoolean => dc
    }

    def getSparkCompatiblePrimitiveTypes: types.DataType = dc.dataType match {
      case DString => StringType
      case DInteger => IntegerType
      case DLong => LongType
      case DDouble => DoubleType
      case DBoolean => BooleanType
    }
  }

  def getInteger(values: List[String]): List[java.lang.Integer] =
    values.map(x => if (x == null) null else new java.lang.Integer(x.toInt))

  def getDouble(values: List[String]): List[java.lang.Double] =
    values.map(x => if (x == null) null else new java.lang.Double(x.toDouble))

  def getBoolean(values: List[String]): List[java.lang.Boolean] =
    values.map(x => if (x == null) null else new java.lang.Boolean(x.toBoolean))

  def getLong(values: List[String]): List[java.lang.Long] =
    values.map(x => if (x == null) null else new java.lang.Long(x.toLong))
}