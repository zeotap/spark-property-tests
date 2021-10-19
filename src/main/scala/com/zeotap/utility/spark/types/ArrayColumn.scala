package com.zeotap.utility.spark.types

import com.zeotap.utility.spark.example.helper.ColumnConstants.DefaultCollectionSize
import com.zeotap.utility.spark.ops.DataColumnOps.DataColumnUtils
import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types.DataColumn.dataColumn
import org.apache.spark.sql.types.{DataType => _, _}
import org.scalacheck.Gen

case class ArrayColumn(dataColumn: DataColumn, maxArraySize: Int) extends DColumn {
  override def generateSchema: StructField = StructField(dataColumn.name, ArrayType(dataColumn.getSparkCompatiblePrimitiveTypes), true)

  override def getName: String = dataColumn.getName

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

  def arrayColumn(dataColumn: DataColumn, maxArraySize: Int = DefaultCollectionSize): ArrayColumn = ArrayColumn(dataColumn, maxArraySize)

  def arrayColumn(name: String, dataType: DataType, options: DataOption, values: List[String], maxArraySize: Int): ArrayColumn =
    ArrayColumn(dataColumn(name, dataType, options, values), maxArraySize)

  def arrayColumn(name: String, dataType: DataType, options: DataOption, values: List[String]): ArrayColumn =
    ArrayColumn(dataColumn(name, dataType, options, values), DefaultCollectionSize)
}
