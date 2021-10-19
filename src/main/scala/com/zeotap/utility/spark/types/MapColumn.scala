package com.zeotap.utility.spark.types

import com.zeotap.utility.spark.example.helper.ColumnConstants.DefaultCollectionSize
import com.zeotap.utility.spark.ops.DataColumnOps.DataColumnUtils
import com.zeotap.utility.spark.traits.{AlwaysPresent, DColumn, DataType}
import com.zeotap.utility.spark.types.DataColumn.dataColumn
import org.apache.spark.sql.types.{MapType, StructField}
import org.scalacheck.Gen

case class MapColumn(name: String, key: DataColumn, value: DataColumn, maxMapSize: Int) extends DColumn {
  override def generateSchema: StructField = StructField(name,
    MapType(key.getSparkCompatiblePrimitiveTypes, value.getSparkCompatiblePrimitiveTypes, false), true)

  override def getName: String = name

  override def dataGenerator[A]: Gen[A] = {
    val tupleGen = for {
      k <- key.dataGenerator[Any]
      v <- value.dataGenerator[Any]
    } yield Tuple2(k, v).asInstanceOf[(Any, Any)]

    val containerSize = scala.util.Random.nextInt(maxMapSize)
    Gen.mapOfN(containerSize, tupleGen)
  }.asInstanceOf[Gen[A]]
}

object MapColumn {
  def mapColumn(name: String, key: DataColumn, value: DataColumn, maxMapSize: Int = DefaultCollectionSize): MapColumn =
    MapColumn(name, key, value, maxMapSize)

  def mapColumn(name: String, keys: List[String], values: List[String], keyType: DataType,
                valueType: DataType): MapColumn = MapColumn(name,
    dataColumn(name, keyType, AlwaysPresent, keys), dataColumn(name, valueType, AlwaysPresent, values), DefaultCollectionSize)

  def mapColumn(name: String, keys: List[String], values: List[String], keyType: DataType,
                valueType: DataType, maxMapSize: Int): MapColumn = MapColumn(name,
    dataColumn(name, keyType, AlwaysPresent, keys), dataColumn(name, valueType, AlwaysPresent, values), maxMapSize)
}
