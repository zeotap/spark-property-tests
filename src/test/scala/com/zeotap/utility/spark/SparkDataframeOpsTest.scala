package com.zeotap.utility.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.utility.spark.example.generator.RandomDataGenerator
import com.zeotap.utility.spark.example.helper.ColumnConstants.JavaNull
import com.zeotap.utility.spark.example.helper.UserDefinedColumns._
import com.zeotap.utility.spark.example.types.CookieArrayColumn
import com.zeotap.utility.spark.example.types.CookieArrayColumn.cookieArrayColumn
import com.zeotap.utility.spark.ops.DataColumnOps._
import com.zeotap.utility.spark.ops.SparkDataframeOps.SparkOps
import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types.ArrayColumn.arrayColumn
import com.zeotap.utility.spark.types.DataColumn._
import com.zeotap.utility.spark.types.MapColumn.mapColumn
import com.zeotap.utility.spark.types._
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.types.{DataType => _, _}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.scalacheck.Prop.forAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers.check

class SparkDataframeOpsTest extends AnyFunSuite with DataFrameSuiteBase {

  test("test primitive and array column generation - schema and data") {
    val dataColumns = SparkDataframe(
      zuid(AlwaysSkewed),
      gender().withJunk,
      rawIAB().withNull.withJunk,
      age().asString,
      otr(AlwaysUniform),
      countryCode(AlwaysUniform).withNull,
      dataColumn("Income_preprocess", DInteger, AlwaysPresent, List("32000", "20000", "45000", "70000")).withNull,
      dataColumn("Common_ts", DLong, AlwaysSkewed, RandomDataGenerator.timestamp(15)),
      dataColumn("creditCardAvailable", DBoolean, AlwaysUniform, List("True", "False")),
      arrayColumn(dataColumn("Income_preprocess_array", DInteger, AlwaysSkewed, List("32000", "20000", "45000", "70000", null)), 10000),
      arrayColumn(adid().withNull.withJunk),
      mapColumn("adid_gender_map", adid(AlwaysPresent, RandomDataGenerator.UUID(5000)), bundleid(AlwaysSkewed), 1000),
      mapColumn("adid_to_age_mapping", adid().values, age().values, DString, DInteger),
      cookieArrayColumn()
    )
    implicit val sc: SparkSession = spark
    val prop = forAll(dataColumns.getArbitraryGenerator()) { df =>
      !testValues(dataColumns, df).exists(_ != true) && !testSchema(dataColumns, df).exists(_ != true)
    }
    check(prop)
  }

  def testValues(sparkDataframe: SparkDataframe, df: DataFrame): List[Boolean] = sparkDataframe.dataColumns.map {
    case d: DataColumn => testDataColumnValues(df, d)
    case a: ArrayColumn => testArrayColumnValues(df, a)
    case m: MapColumn => if (df.count() > 0) testNonPrimitiveColumnValues(df, m) else true
    case ca: CookieArrayColumn => if (df.count() > 0) testNonPrimitiveColumnValues(df, ca) else true
  }.toList

  /**
   * Map Column and Cookie Array test
   *  1. Populates key-value pair/cookie array column based on input DataColumns or List of String
   *  2. Empty maps/cookie array column may be present but no Java Null as instance reference
   *  3. AlwaysSkewed or AlwaysUniform - is not supported for MapColumn/Cookie Array type
   * @param df DataFrame
   * @param d  MapColumn  or CookieArrayColumn
   * @return result as Boolean
   */
  // TODO : The AlwaysSkewed feature does not make a lot of sense now. Define Skew in Map and come up with a better implementation
  def testNonPrimitiveColumnValues(df: DataFrame, d: DColumn): Boolean = df.map(row => {
    val nonPrimitiveColumn = d match {
      case m: MapColumn => row.getAs[Map[Any, Any]](m.name)
      case c: CookieArrayColumn => row.getAs[Seq[Row]](c.getName)
    }
    if (nonPrimitiveColumn.isEmpty) {
      nonPrimitiveColumn != JavaNull
    } else {
      nonPrimitiveColumn.nonEmpty
    }
  })(Encoders.scalaBoolean).reduce(_ && _)

  /**
   * Array Column test
   * 1. Uniformity Test is not very robust. Our observation is most of the times it is uniform but at times, can be skewed as well
   * 2. For Skew test, following was observed
   *    a. In a dataframe array column, the ratio of the most frequent element to the least frequent element
   *       is greater than or equal to 1:3
   *    b. We observed both cases of only skewed array as well as only uniform array being present at times with no certainty
   * // TODO : The AlwaysSkewed feature does not make a lot of sense now. Define Skew in Array and come up with a better implementation
   *
   * @param df DataFrame
   * @param a  ArrayColumn
   * @return result as Boolean
   */
  def testArrayColumnValues(df: DataFrame, a: ArrayColumn): Boolean = {
    if (df.count() == 0)
      true
    else {
      val resultDF = df.map(x => {
        val arrayColumnValues = x.getAs[Seq[Any]](a.dataColumn.name)
        val countMap = arrayColumnValues.groupBy(identity).mapValues(_.size)

        if (countMap.isEmpty) {
          a.dataColumn.options match {
            case AlwaysPresent => false
            case AlwaysUniform => false
            case AlwaysSkewed => true
          }
        } else {
          val minTuple = countMap.minBy(_._2)
          val maxTuple = countMap.maxBy(_._2)
          maxTuple._2 >= minTuple._2 * 3
        }
      })(Encoders.scalaBoolean)
      val actualResultDF = resultDF.groupBy("value").count()
      a.dataColumn.options match {
        case AlwaysSkewed => val rows = actualResultDF.collect()
          if (rows.length == 1) {
            true
          } else {
            assert(rows.length == 2)
            val first = rows(0)
            val second = rows(1)
            val firstBool = first.getAs("value").asInstanceOf[Boolean]
            val secondBool = second.getAs("value").asInstanceOf[Boolean]
            val firstCount = first.getAs("count").asInstanceOf[Long]
            val secondCount = second.getAs("count").asInstanceOf[Long]
            (firstBool, secondBool) match {
              case (true, false) => firstCount >= secondCount
              case (false, true) => secondCount >= firstCount
            }
          }
        case _ => true
      }
    }
  }

  private def testDataColumnValues(df: DataFrame, dc: DataColumn) = {
    dc.dataType match {
      case DString => primitiveColumnCountCheck(dc, df, dc.values)
      case DInteger => primitiveColumnCountCheck(dc, df, getInteger(dc.values))
      case DBoolean => primitiveColumnCountCheck(dc, df, getBoolean(dc.values))
      case DLong => primitiveColumnCountCheck(dc, df, getLong(dc.values))
      case DDouble => primitiveColumnCountCheck(dc, df, getDouble(dc.values))
    }
  }

  def primitiveColumnCountCheck[A](x: DataColumn, df: DataFrame, values: List[A]): Boolean = x.options match {
    case AlwaysPresent | AlwaysUniform => assertTotalCountEqualsFilterCount(x.name, values, df)
    case AlwaysSkewed => assertCountsSkewedDistribution(x.name, df)
  }

  def assertTotalCountEqualsFilterCount[A](colName: String, values: List[A], df: DataFrame) = {
    if (values.contains(null))
      df.filter(col(colName).isin(values: _*) or col(colName).isNull).count == df.count
    else
      df.filter(col(colName).isin(values: _*)).count == df.count
  }

  def assertCountsSkewedDistribution(colName: String, df: DataFrame) = {
    if (df.select(colName).distinct().count() <= 4) true
    else {
      val grouped = df.groupBy(colName).count.agg(min("count"), max("count")).head()
      1.2 * grouped.getLong(0) <= grouped.getLong(1) || grouped.getLong(0) == grouped.getLong(1)
    }
  }

  def testSchema(sparkDataframe: SparkDataframe, df: DataFrame) = {
    val sparkDataTypes = df.schema.fields.map(f => f.dataType)
    (sparkDataframe.dataColumns zip sparkDataTypes).map {
      case (a: ArrayColumn, b: ArrayType) => primitiveSchemaCheck(a.dataColumn.dataType, b.elementType)
      case (d: DataColumn, b) => primitiveSchemaCheck(d.dataType, b)
      case (m: MapColumn, b: MapType) => primitiveSchemaCheck(m.key.dataType, b.keyType) && primitiveSchemaCheck(m.value.dataType, b.valueType)
      case (c: CookieArrayColumn, b: ArrayType) => cookieArraySchemaCheck(b, c)
    }
  }.toList

  def cookieArraySchemaCheck(sparkDataType: ArrayType, dType: CookieArrayColumn) = sparkDataType.elementType.isInstanceOf[StructType] && {
    val structType = sparkDataType.elementType.asInstanceOf[StructType]
    structType.size == 2 &&
      structType.fields(0).name.equalsIgnoreCase("id_type") &&
      structType.fields(1).name.equalsIgnoreCase("id_value") &&
      primitiveSchemaCheck(dType.idType.dataType, structType.fields(0).dataType) &&
      primitiveSchemaCheck(dType.idValue.dataType, structType.fields(1).dataType)
  }

  def primitiveSchemaCheck(dataColumnDType: com.zeotap.utility.spark.traits.DataType, sparkDataType: org.apache.spark.sql.types.DataType) = dataColumnDType match {
    case DString => sparkDataType == StringType
    case DBoolean => sparkDataType == BooleanType
    case DDouble => sparkDataType == DoubleType
    case DLong => sparkDataType == LongType
    case DInteger => sparkDataType == IntegerType
  }
}
