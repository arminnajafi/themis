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

package io.themis.spark.iceberg

class IcebergDatagenConfig(val args: Array[String]) extends Serializable {

  // type of generator, tpcds or random
  var gen = "tpcds"

  // if should replace the table if exists
  // this is a DSv2 only feature compared to writer.saveAsTable in DSv1
  // thus only in Iceberg config
  var replaceTable = false

  // parquet, orc, avro
  var fileFormat: String = "parquet"

  // compression codec for the file format
  var compressionCodec: String = "zstd"

  var objectStorageMode = false

  // for testing only, set Spark master as local[2] and use local FileIO
  var local = false

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case "--gen" :: value :: tail =>
          gen = value
          args = tail

        case "--replace-tables" :: tail =>
          replaceTable = true
          args = tail

        case "--file-format" :: value :: tail =>
          fileFormat = value
          args = tail

        case "--compression-codec" :: value :: tail =>
          compressionCodec = value
          args = tail

        case "--object-storage-mode" :: tail =>
          objectStorageMode = true
          args = tail

        case "--local" :: tail =>
          local = true
          args = tail

        case _ :: tail =>
          args = tail
      }
    }
  }

  private def validateArguments(): Unit = {
    // no op
  }
}
