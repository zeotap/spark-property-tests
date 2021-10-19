package com.zeotap.utility.spark.traits

import org.scalacheck.Gen

trait DataGenerator[A] {
  def get(data: DataOption, values: List[A]): Gen[A]
}
object DataGenerator{
  def apply[A](implicit instance:DataGenerator[A]): DataGenerator[A] = instance
}
