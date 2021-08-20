package com.zeotap.utility.spark.traits

sealed trait DataOption

final case object AlwaysPresent extends DataOption

//  Using AlwaysUniform one can expect approximately uniform distribution
//          +-----------------+-----+         +-----------------+-----+
//          |Income_preprocess|count|         |Income_preprocess|count|
//          +-----------------+-----+         +-----------------+-----+
//          |              0.7|    5|         |              0.7|   18|
//          |              0.1|    5|         |              0.1|   14|
//          |              0.8|    3|         |              0.8|   20|
//          |              0.4|    7|         |              0.4|   18|
//          +-----------------+-----+         +-----------------+-----+

final case object AlwaysUniform extends DataOption

//  Using AlwaysSkewed one can expect a distribution having two values with frequency 6-10 times the others
//          +-----------------+-----+           +-----------------+-----+         +-----------------+-----+
//          |Income_preprocess|count|           |Income_preprocess|count|         |Income_preprocess|count|
//          +-----------------+-----+           +-----------------+-----+         +-----------------+-----+
//          |              0.7|    4|           |              0.7|    4|         |              0.7|    2|
//          |              0.1|   14|           |              0.1|   23|         |              0.1|   12|
//          |              0.8|    2|           |              0.8|    4|         |              0.4|    6|
//          |              0.4|   10|           |              0.4|   19|         +-----------------+-----+
//          +-----------------+-----+           +-----------------+-----+

final case object AlwaysSkewed extends DataOption
