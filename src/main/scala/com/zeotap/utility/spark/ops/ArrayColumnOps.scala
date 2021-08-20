package com.zeotap.utility.spark.ops

import com.zeotap.utility.spark.ops.DataColumnOps.DataColumnUtils
import com.zeotap.utility.spark.types.ArrayColumn

object ArrayColumnOps {
  implicit class ArrayColumnExt(arr: ArrayColumn) {
    def withJunk = arr.copy(dataColumn = arr.dataColumn.withJunk)

    def withNull = arr.copy(dataColumn = arr.dataColumn.withNull)
  }
}
