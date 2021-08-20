package com.zeotap.utility.spark.traits

sealed trait DataType

final case object DString extends DataType

final case object DInteger extends DataType

final case object DBoolean extends DataType

final case object DDouble extends DataType

final case object DLong extends DataType
