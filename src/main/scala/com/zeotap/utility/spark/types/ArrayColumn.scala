package com.zeotap.utility.spark.types

import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types.DataColumn.dataColumn
import org.apache.spark.sql.types.{DataType => _, _}
import org.scalacheck.Gen

case class ArrayColumn(dataColumn: DataColumn, maxArraySize: Int) extends DColumn {
  override def generateSchema: StructField = dataColumn.dataType match {
    case DString => StructField(dataColumn.name, ArrayType(StringType), true)
    case DBoolean => StructField(dataColumn.name, ArrayType(BooleanType), true)
    case DDouble => StructField(dataColumn.name, ArrayType(DoubleType), true)
    case DLong => StructField(dataColumn.name, ArrayType(LongType), true)
    case DInteger => StructField(dataColumn.name, ArrayType(IntegerType), true)
  }

  override def dataGenerator[A]: Gen[A] = {
    val containerSize = scala.util.Random.nextInt(maxArraySize)
    dataColumn.dataType match {
      case DString => Gen.containerOfN[Array, String](containerSize, dataColumn.dataGenerator.asInstanceOf[Gen[String]])
      case DBoolean => Gen.containerOfN[Array, java.lang.Boolean](containerSize, dataColumn.dataGenerator.asInstanceOf[Gen[java.lang.Boolean]])
      case DDouble => Gen.containerOfN[Array, java.lang.Double](containerSize, dataColumn.dataGenerator.asInstanceOf[Gen[java.lang.Double]])
      case DLong => Gen.containerOfN[Array, java.lang.Long](containerSize, dataColumn.dataGenerator.asInstanceOf[Gen[java.lang.Long]])
      case DInteger => Gen.containerOfN[Array, java.lang.Integer](containerSize, dataColumn.dataGenerator.asInstanceOf[Gen[java.lang.Integer]])
    }
  }.asInstanceOf[Gen[A]]
}

object ArrayColumn {
  def arrayColumn(dataColumn: DataColumn): ArrayColumn = ArrayColumn(dataColumn, 120)

  def arrayColumn(dataColumn: DataColumn, maxArraySize: Int): ArrayColumn = ArrayColumn(dataColumn, maxArraySize)

  def arrayColumn(name: String, dataType: DataType, options: DataOption, values: List[String]): ArrayColumn =
    ArrayColumn(dataColumn(name, dataType, options, values), 120)

  def arrayColumn(name: String, dataType: DataType, options: DataOption, values: List[String], maxArraySize: Int): ArrayColumn =
    ArrayColumn(dataColumn(name, dataType, options, values), maxArraySize)
}
