package com.zeotap.utility.spark.example.types

import com.zeotap.utility.spark.example.helper.ColumnConstants.ID_TYPE
import com.zeotap.utility.spark.example.helper.ColumnConstants
import com.zeotap.utility.spark.traits.DColumn
import com.zeotap.utility.spark.types.DataColumn
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalacheck.Gen


case class CookieArrayColumn(name: String, idType: DataColumn,
                             idValue: DataColumn, maxSize: Int) extends DColumn {

  override def generateSchema: StructField = StructField(name,
    ArrayType(new StructType().add("id_type", StringType).add("id_value", StringType)))

  override def getName: String = name

  override def dataGenerator[A]: Gen[A] = {
    val arrayGen = for {
      k <- idType.dataGenerator[String]
      v <- idValue.dataGenerator[String]
    } yield Row(k, v)
    Gen.containerOfN[Array, Row](maxSize, arrayGen)
  }.asInstanceOf[Gen[A]]
}

object CookieArrayColumn {
  def cookieArrayColumn(name: String = "_cookieArray", maxSize: Int = 5)
  = CookieArrayColumn(name, ID_TYPE, ColumnConstants.ADID.copy(name = "id_value"), maxSize)
}
