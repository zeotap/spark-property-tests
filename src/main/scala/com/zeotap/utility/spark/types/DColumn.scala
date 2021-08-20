package com.zeotap.utility.spark.types

import org.apache.spark.sql.types.StructField
import org.scalacheck.Gen

trait DColumn {
  def generateSchema: StructField

  def dataGenerator[A]: Gen[A]
}
