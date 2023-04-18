/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.themis.spark.tpcds

import scala.util.Try

class SparkTpcdsRowGeneratorConfig(val args: Array[String]) extends Serializable {

  // variant of TPCDS dataset to use
  // default: 7 partitioned tables (3 x _sales, 3 x _returns, inventory),
  //          partitioned by int date,
  //          other tables are all unpartitioned
  // unpartitioned: all tables are unpartitioned
  var variant: String = "default"

  // a comma separated list of tables to filter
  // if specified, other tables will not be generated
  var filter: String = _

  // scale factor for TPCDS, 1 = 1GB data
  // typically divide by 3 for actual data file size
  var scaleFactor = 1

  // number of parallel workers to generate data
  // note that it does not guarantee every thread generates the same amount of data
  // typically only inventory and customer_demographics have even distributions
  var parallelism = 1

  // if Spark should repartition unpartitioned table into 1 file
  // and repartition partitioned table based on partition key
  var repartitionTables = false

  // if should use string data type for char, CHAR type pads the data
  var useStringForChar = false

  // if should use double data type for decimal. DOUBLE change precision handling of data
  var useDoubleForDecimal = false

  // if execution should filter out null partition values
  var filterOutNullPartitionValues = false

  // if execution should cache data frame, which improves execution speed
  var cacheDF = false

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case "--variant" :: value :: tail =>
          variant = value
          args = tail

        case "--filter" :: value :: tail =>
          filter = value
          args = tail

        case "--scale-factor" :: value :: tail =>
          scaleFactor = value.toInt
          args = tail

        case "--parallelism" :: value :: tail =>
          parallelism = value.toInt
          args = tail

        case "--use-string-for-char" :: tail =>
          useStringForChar = true
          args = tail

        case "--use-double-for-decimal" :: tail =>
          useDoubleForDecimal = true
          args = tail

        case "--repartition-tables" :: tail =>
          repartitionTables = true
          args = tail

        case "--filter-out-null-partition-values" :: tail =>
          filterOutNullPartitionValues = true
          args = tail

        case "--cache-df" :: tail =>
          cacheDF = true
          args = tail

        case _ :: tail =>
          args = tail
      }
    }
  }

  private def validateArguments(): Unit = {
    if (Try(scaleFactor).getOrElse(-1) <= 0) {
      System.err.println("Scale factor must be a positive number")
      System.exit(-1)
    }

    if (Try(parallelism).getOrElse(-1) <= 0) {
      System.err.println("Number of parallelism must be a positive number")
      System.exit(-1)
    }
  }
}
