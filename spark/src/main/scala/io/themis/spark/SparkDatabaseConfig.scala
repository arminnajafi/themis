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

package io.themis.spark

import org.apache.spark.sql.SparkSession

import scala.util.Try

class SparkDatabaseConfig(val args: Array[String]) extends Serializable {

  // database to create the random dataset
  var databaseName: String = "themis"

  // location of the database
  // tables should by default be created at databaseLocation/<table_name>
  var databaseLocation: String = _

  // if we should drop the old database if it exists
  // the drop will be cascade
  var dropDatabaseIfExists = false

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {

        case "--database-name" :: value :: tail =>
          databaseName = value
          args = tail

        case "--database-location" :: value :: tail =>
          databaseLocation = value
          args = tail

        case "--drop-database-if-exists" :: tail =>
          dropDatabaseIfExists = true
          args = tail

        case _ :: tail =>
          args = tail
      }
    }
  }

  private def validateArguments(): Unit = {
    // no-ops
  }

  def ensureDatabaseExists(spark: SparkSession, catalog: String): Unit = {
    if (dropDatabaseIfExists) {
      spark.sql(s"DROP DATABASE IF EXISTS ${catalog}.${databaseName} CASCADE")
    }

    if (databaseLocation == null) {
      throw new IllegalArgumentException("Database location must not be null")
    }
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${catalog}.${databaseName}" +
      s" LOCATION '${databaseLocation}'")
    print(s"Executed create database for $databaseName at $databaseLocation")
  }
}
