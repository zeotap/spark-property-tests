# spark-property-tests
Write property based tests easily on spark dataframes

## Why
While writing tests for Spark code, we tend to write a lot of boilerplate just to create a test spark dataframe initialised with some test data. Not only were these test sets not readable, but they also do not adhere to Property-based testing standards. 

We needed a utility that would have 

🥇 less boilerplate code

🥇 easily extensible interface for your custom use-cases

🥇 easily build out-of-box support for most common attributes in your data/project

🥇 promote usage of Property-based tests

This utility is based on the [spark-testing-base library by Holden Karau](https://github.com/holdenk/spark-testing-base)

## Usage
Please go through the [Wiki](https://github.com/zeotap/spark-property-tests/wiki) to understand the usage of the library. 

We have made use of Typeclasses in Scala and Generators in [scalacheck](https://github.com/typelevel/scalacheck/blob/main/doc/UserGuide.md) to present some simple interfaces to write easy property-based-tests in spark.

Additionally, we have provided examples of how you can leverage the library for your own organization under package `com.zeotap.utility.spark.example`

## Dependency Management
We are activly working on [OSSRH-78645](https://issues.sonatype.org/browse/OSSRH-78645) and will publish the details here once the artifacts are available in Maven Central 

## Build
Project is build using `sbt`
