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

package io.themis.spark.datagen

import scala.util.Try

class SparkRandomRowGeneratorConfig(val args: Array[String]) extends Serializable {

  // see datasets in org.apache.themis.Datasets
  var dataset: String = "simple"

  // a comma separated list of tables to filter
  // if specified, other tables will not be generated
  var filter: String = _

  var rowCount = 100L

  // number of parallel workers to generate data
  var parallelism = 1

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case "--dataset" :: value :: tail =>
          dataset = value
          args = tail

        case "--filter" :: value :: tail =>
          filter = value
          args = tail

        case "--row-count" :: value :: tail =>
          rowCount = value.toLong
          args = tail

        case "--parallelism" :: value :: tail =>
          parallelism = value.toInt
          args = tail

        case _ :: tail =>
          args = tail
      }
    }
  }

  private def validateArguments(): Unit = {
    if (Try(rowCount).getOrElse(-1L) <= 0L) {
      System.err.println("Row count must be a positive number")
      System.exit(-1)
    }

    if (Try(parallelism).getOrElse(-1) <= 0) {
      System.err.println("Number of parallelism must be a positive number")
      System.exit(-1)
    }
  }
}
