package com.zeotap.utility.spark.traits

import org.apache.spark.sql.types.StructField
import org.scalacheck.Gen

trait DColumn {
  def generateSchema: StructField

  def getName: String

  def dataGenerator[A]: Gen[A]
}
