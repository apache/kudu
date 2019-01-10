// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations

package org.apache.kudu.spark.examples

import collection.JavaConverters._

import org.slf4j.LoggerFactory

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object SparkExample {

  // The list of RPC endpoints of Kudu masters in the cluster,
  // separated by comma. The default value assumes the following:
  //   * the cluster runs a single Kudu master
  //   * the Kudu master runs at the default RPC port
  //   * the spark-submit is run at the Kudu master's node
  val kuduMasters: String = System.getProperty("kuduMasters", "localhost:7051")

  // The name of a table to create.
  val tableName: String = System.getProperty("tableName", "kudu_spark_example")

  // The replication factor of the table to create. Make sure at least this
  // number of tablet servers are available when running this example app.
  // Replication factors of 1, 3, 5, 7 are available out-of-the-box.
  // The default value of 1 is chosen to provide maximum environment
  // compatibility, but it's good only for small toy examples like this.
  // For real-world scenarios the replication factor of 1 (i.e. keeping table's
  // data non-replicated) is a bad idea in general: consider the replication
  // factor of 3 and higher.
  val tableNumReplicas: Int = Integer.getInteger("tableNumReplicas", 1)

  val nameCol = "name"
  val idCol = "id"

  val logger = LoggerFactory.getLogger(SparkExample.getClass)

  // Define a class that we'll use to insert data into the table.
  // Because we're defining a case class here, we circumvent the need to
  // explicitly define a schema later in the code, like during our RDD -> toDF()
  // calls later on.
  case class User(name:String, id:Int)

  def main(args: Array[String]) {
    // Define our session and context variables for use throughout the program.
    // The kuduContext is a serializable container for Kudu client connections,
    // while the SparkSession is the entry point to SparkSQL and
    // the Dataset/DataFrame API.
    val spark = SparkSession.builder.appName("KuduSparkExample").getOrCreate()
    val kuduContext = new KuduContext(kuduMasters, spark.sqlContext.sparkContext)

    // Import a class from the SparkSession we instantiated above, to allow
    // for easier RDD -> DF conversions.
    import spark.implicits._

    // The schema of the table we're going to create.
    val schema = StructType(
                       List(
                         StructField(idCol, IntegerType, false),
                         StructField(nameCol, StringType, false)
                       )
                  )

    var tableIsCreated = false
    try {
      // Make sure the table does not exist. This is mostly to demonstrate
      // the capabilities of the API. In general, there might be a racing
      // request to create the table coming from elsewhere, so even
      // if tableExists() returned false at this time, the table might appear
      // while createTable() is running below. In the latter case, appropriate
      // Kudu exception will be thrown by createTable().
      if (kuduContext.tableExists(tableName)) {
        throw new RuntimeException(tableName + ": table already exists")
      }

      // Create the table with 3 hash partitions, resulting in 3 tablets,
      // each with the specified number of replicas.
      kuduContext.createTable(tableName, schema, Seq(idCol),
        new CreateTableOptions()
          .addHashPartitions(List(idCol).asJava, 3)
          .setNumReplicas(tableNumReplicas))
      tableIsCreated = true

      // Write to the table.
      logger.info(s"writing to table '$tableName'")
      val data = Array(User("userA", 1234), User("userB", 5678))
      val userRDD = spark.sparkContext.parallelize(data)
      val userDF = userRDD.toDF()
      kuduContext.insertRows(userDF, tableName)

      // Read from the table using an RDD.
      logger.info(s"reading back the rows just written")
      val readCols = Seq(nameCol, idCol)
      val readRDD = kuduContext.kuduRDD(spark.sparkContext, tableName, readCols)
      val userTuple = readRDD.map { case Row(name: String, id: Int) => (name, id) }
      userTuple.collect().foreach(println(_))

      // Upsert some rows.
      logger.info(s"upserting to table '$tableName'")
      val upsertUsers = Array(User("newUserA", 1234), User("userC", 7777))
      val upsertUsersRDD = spark.sparkContext.parallelize(upsertUsers)
      val upsertUsersDF = upsertUsersRDD.toDF()
      kuduContext.upsertRows(upsertUsersDF, tableName)

      // Read the updated table using SparkSQL.
      val sqlDF = spark.sqlContext.read.options(
        Map("kudu.master" -> kuduMasters, "kudu.table" -> tableName)).kudu
      sqlDF.createOrReplaceTempView(tableName)
      spark.sqlContext.sql(s"SELECT * FROM $tableName WHERE $idCol > 1000").show
    } catch {
      // Catch, log and re-throw. Not the best practice, but this is a very
      // simplistic example.
      case unknown : Throwable => logger.error(s"got an exception: " + unknown)
      throw unknown
    } finally {
      // Clean up.
      if (tableIsCreated) {
        logger.info(s"deleting table '$tableName'")
        kuduContext.deleteTable(tableName)
      }
      logger.info(s"closing down the session")
      spark.close()
    }
  }
}
