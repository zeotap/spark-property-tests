package com.zeotap.utility.spark.types

import com.zeotap.utility.spark.ops.DataColumnOps.{getBoolean, getDouble, getInteger, getLong}
import com.zeotap.utility.spark.ops.DataGenerationOps
import com.zeotap.utility.spark.traits._
import org.apache.spark.sql.types.{DataType => _, _}
import org.scalacheck.Gen

case class DataColumn(name: String, dataType: DataType, options: DataOption, values: List[String]) extends DColumn {
  override def generateSchema: StructField = dataType match {
    case DString => StructField(name, StringType, true)
    case DBoolean => StructField(name, BooleanType, true)
    case DDouble => StructField(name, DoubleType, true)
    case DLong => StructField(name, LongType, true)
    case DInteger => StructField(name, IntegerType, true)
  }

  override def dataGenerator[A]: Gen[A] = {
    import DataGenerationOps._
    dataType match {
      case DString => DataGenerator[String].get(options, values)
      case DBoolean => DataGenerator[java.lang.Boolean].get(options, getBoolean(values))
      case DDouble => DataGenerator[java.lang.Double].get(options, getDouble(values))
      case DLong => DataGenerator[java.lang.Long].get(options, getLong(values))
      case DInteger => DataGenerator[java.lang.Integer].get(options, getInteger(values))
    }
  }.asInstanceOf[Gen[A]]
}

object DataColumn {
  def dataColumn(name: String, datatype: DataType, options: DataOption, values: List[String]) =
    DataColumn(name, datatype, options, values)
}
