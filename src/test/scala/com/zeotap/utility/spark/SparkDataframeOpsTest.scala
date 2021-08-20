package com.zeotap.utility.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.utility.spark.example.CommonColumnHelper._
import com.zeotap.utility.spark.generator.RandomDataGenerator
import com.zeotap.utility.spark.ops.DataColumnOps._
import com.zeotap.utility.spark.ops.SparkDataframeOps.SparkOps
import com.zeotap.utility.spark.traits._
import com.zeotap.utility.spark.types.ArrayColumn.arrayColumn
import com.zeotap.utility.spark.types.DataColumn._
import com.zeotap.utility.spark.types.{ArrayColumn, DataColumn, SparkDataframe}
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers.check

import scala.collection.mutable

class SparkDataframeOpsTest extends FunSuite with DataFrameSuiteBase {

  test("test primitive and array column generation - schema and data") {
    val dataColumns = types.SparkDataframe(
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
      arrayColumn(adid().withNull.withJunk)
    )
    implicit val sc = spark
    val prop = forAll(dataColumns.getArbitraryGenerator()) {
      df =>
        !testValues(dataColumns, df).exists(_ != true) && !testSchema(dataColumns, df).exists(_ != true)
    }
    check(prop)
  }

  def testValues(sparkDataframe: SparkDataframe, df: DataFrame): List[Boolean] = sparkDataframe.dataColumns.map {
    case d: DataColumn => testDataColumnValues(df, d)
    case a: ArrayColumn => testArrayColumnValues(df, a)
  }.toList

  /**
   * Array Column test
   * 1. Uniformity Test is not very robust. Our observation is most of the times it is uniform but at times, can be skewed as well
   * 2. For Skew test, following was observed
   *    a. In a dataframe array column, the ratio of the most frequent element to the least frequent element
   *       is greater than or equal to 1:3
   *    b. We observed both cases of only skewed array as well as only uniform array being present at times with no certainty
   * // TODO : This feature does not make a lot of sense now. Define Skew in Array and come up with a better implementation
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
        val wrappedArray = x.getAs[mutable.WrappedArray[Any]](a.dataColumn.name)
        val countMap = wrappedArray.groupBy(identity).mapValues(_.size)

        val order = new Ordering[(Any, Int)] {
          override def compare(x: (Any, Int), y: (Any, Int)): Int = if (x._2 > y._2) 1
          else if (x._2 < y._2) -1
          else 0
        }
        if (countMap.isEmpty) {
          a.dataColumn.options match {
            case AlwaysPresent => false.asInstanceOf[java.lang.Boolean]
            case AlwaysUniform => false.asInstanceOf[java.lang.Boolean]
            case AlwaysSkewed => true.asInstanceOf[java.lang.Boolean]
          }
        } else {
          val minTuple = countMap.min(order)
          val maxTuple = countMap.max(order)
          (maxTuple._2 >= minTuple._2 * 3).asInstanceOf[java.lang.Boolean]
        }
      })(Encoders.BOOLEAN)
      resultDF.groupBy("value").count().show()
      val actualResultDF = resultDF.groupBy("value").count()
      a.dataColumn.options match {
        case AlwaysSkewed => if (actualResultDF.count() == 1) {
          true
        } else {
          actualResultDF.reduce((a, b) => {
            val firstBool = a.getAs("value").asInstanceOf[Boolean]
            val secondBool = b.getAs("value").asInstanceOf[Boolean]
            val firstCount = a.getAs("count").asInstanceOf[Long]
            val secondCount = b.getAs("count").asInstanceOf[Long]
            (firstBool, secondBool) match {
              case (true, false) => Row(firstCount >= secondCount)
              case (false, true) => Row(secondCount >= firstCount)
            }
          }).get(0).asInstanceOf[Boolean]
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

  def arraySchemaCheck(a: ArrayColumn, dataframeNestedSchemaTypeAsString: String): Boolean = extractComplexType(dataframeNestedSchemaTypeAsString).equals("ArrayType") &&
    primitiveSchemaCheck(a.dataColumn.dataType, extractInnerPrimitiveType(dataframeNestedSchemaTypeAsString))


  def extractComplexType(ct: String) = ct.split("\\(")(0)

  def extractInnerPrimitiveType(ct: String) = ct.split("\\(")(1).split(",")(0)

  def testSchema(dataColumns: SparkDataframe, df: DataFrame): List[Boolean] =
    (dataColumns.dataColumns zip df.dtypes).map {
      case (d: DataColumn, s: (String, String)) => primitiveSchemaCheck(d.dataType, s._2)
      case (a: ArrayColumn, s: (String, String)) => arraySchemaCheck(a, s._2)
    }.toList

  def primitiveSchemaCheck(dataColumnDType: DataType, dataFrameSchemaDType: String) = dataColumnDType match {
    case DString => dataFrameSchemaDType.equals("StringType")
    case DBoolean => dataFrameSchemaDType.equals("BooleanType")
    case DDouble => dataFrameSchemaDType.equals("DoubleType")
    case DLong => dataFrameSchemaDType.equals("LongType")
    case DInteger => dataFrameSchemaDType.equals("IntegerType")
  }
}
