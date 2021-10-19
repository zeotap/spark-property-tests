package com.zeotap.utility.spark.types


import com.zeotap.utility.spark.ops.DataColumnOps.{DataColumnUtils, getBoolean, getDouble, getInteger, getLong}
import com.zeotap.utility.spark.ops.DataGenerationOps
import com.zeotap.utility.spark.traits._
import org.apache.spark.sql.types.{DataType => _, _}
import org.scalacheck.Gen

case class DataColumn(name: String, dataType: DataType, options: DataOption, values: List[String]) extends DColumn {
  override def generateSchema: StructField = StructField(name, this.getSparkCompatiblePrimitiveTypes, true)

  override def getName: String = name

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

  def stringColumn(name: String, values: List[String]) = DataColumn(name, DString, AlwaysPresent, values)

  def intColumn(name: String, values: List[String]) = DataColumn(name, DInteger, AlwaysPresent, values)

  def boolColumn(name: String, values: List[String]) = DataColumn(name, DBoolean, AlwaysPresent, values)

  def doubleColumn(name: String, values: List[String]) = DataColumn(name, DDouble, AlwaysPresent, values)

  def longColumn(name: String, values: List[String]) = DataColumn(name, DLong, AlwaysPresent, values)
}
