package com.zeotap.utility.spark.ops

import com.zeotap.utility.spark.traits._
import org.scalacheck.Gen

import scala.util.Random


object DataGenerationOps {
  implicit val booleanGen = new DataGenerator[java.lang.Boolean] {
    override def get(data: DataOption, values: List[java.lang.Boolean]): Gen[java.lang.Boolean] = getHelper(data, values)
  }

  implicit val longGen = new DataGenerator[java.lang.Long] {
    override def get(data: DataOption, values: List[java.lang.Long]): Gen[java.lang.Long] = getHelper(data, values)
  }

  implicit val doubleGen = new DataGenerator[java.lang.Double] {
    override def get(data: DataOption, values: List[java.lang.Double]): Gen[java.lang.Double] = getHelper(data, values)
  }

  implicit val integerGen = new DataGenerator[Integer] {
    override def get(data: DataOption, values: List[java.lang.Integer]): Gen[java.lang.Integer] = getHelper(data, values)
  }

  implicit val stringGen = new DataGenerator[String] {
    override def get(data: DataOption, values: List[String]): Gen[String] = getHelper(data, values)
  }

  def getHelper[T](data: DataOption, values: List[T]): Gen[T] = data match {
    case AlwaysPresent => Gen.oneOf(values)
    case AlwaysUniform => generatorWithFrequency(values, List.fill(values.length)(1))
    case AlwaysSkewed => generatorWithFrequency(values, getSkewedFrequency(values.length))
  }

  def getSkewedFrequency(length: Int): List[Int] = List.fill(2)(9) ::: List.fill(length - 2)(Random.nextInt(2))

  def generatorWithFrequency[A](values: List[A], frequency: List[Int]): Gen[A] =
    Gen.frequency((frequency zip Random.shuffle(values)).map(x => (x._1, Gen.oneOf(List(x._2)))): _*)

}
