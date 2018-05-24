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
// under the License.

package org.apache.kudu.spark.tools

import java.net.InetAddress

import org.apache.kudu.client.KuduClient
import org.apache.kudu.spark.tools.ImportExportKudu.ArgsCls
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kudu.spark.kudu._
import org.apache.yetus.audience.{InterfaceAudience, InterfaceStability}

@InterfaceAudience.Public
@InterfaceStability.Unstable //TODO: Unstable due to KUDU-2454
object ImportExportKudu {
  val LOG: Logger = LoggerFactory.getLogger(ImportExportKudu.getClass)

  def fail(msg: String): Nothing = {
    System.err.println(msg)
    sys.exit(1)
  }

  def defaultMasterAddrs: String = InetAddress.getLocalHost.getCanonicalHostName

  def usage: String =
    s"""
       | Usage: --operation=import/export --format=<data-format(csv,parquet,avro)> --master-addrs=<master-addrs> --path=<path> --table-name=<table-name>
       |    where
       |      operation: import or export data from or to Kudu tables, default: import
       |      format: specify the format of data want to import/export, the following formats are supported csv,parquet,avro default:csv
       |      masterAddrs: comma separated addresses of Kudu master nodes, default: $defaultMasterAddrs
       |      path: path to input or output for import/export operation, default: file://
       |      tableName: table name to import/export, default: ""
       |      columns: columns name for select statement on export from kudu table, default: *
       |      delimiter: delimiter for csv import/export, default: ,
       |      header: header for csv import/export, default:false
     """.stripMargin

  case class ArgsCls(operation: String = "import",
                     format: String = "csv",
                     masterAddrs: String = defaultMasterAddrs,
                     path: String = "file://",
                     tableName: String = "",
                     columns: String = "*",
                     delimiter: String = ",",
                     header: String = "false",
                     inferschema: String="false"
                    )

  object ArgsCls {
    private def parseInner(options: ArgsCls, args: List[String]): ArgsCls = {
      LOG.info(args.mkString(","))
      args match {
        case Nil => options
        case "--help" :: _ =>
          System.err.println(usage)
          sys.exit(0)
        case flag :: Nil => fail(s"flag $flag has no value\n$usage")
        case flag :: value :: tail =>
          val newOptions: ArgsCls = flag match {
            case "--operation" => options.copy(operation = value)
            case "--format" => options.copy(format = value)
            case "--master-addrs" => options.copy(masterAddrs = value)
            case "--path" => options.copy(path = value)
            case "--table-name" => options.copy(tableName = value)
            case "--columns" => options.copy(columns = value)
            case "--delimiter" => options.copy(delimiter = value)
            case "--header" => options.copy(header = value)
            case "--inferschema" => options.copy(inferschema = value)
            case _ => fail(s"unknown argument given $flag")
          }
          parseInner(newOptions, tail)
      }
    }

    def parse(args: Array[String]): ArgsCls = {
      parseInner(ArgsCls(), args.flatMap(_.split('=')).toList)
    }
  }
}

object ImportExportFiles {

  import ImportExportKudu.fail

  def run(args: ArgsCls, ss: SparkSession): Unit = {
    val kc = new KuduContext(args.masterAddrs, ss.sparkContext)
    val sqlContext = ss.sqlContext

    val client: KuduClient = kc.syncClient
    if (!client.tableExists(args.tableName)) {
      fail(args.tableName + s" table doesn't exist")
    }

    val kuduOptions = Map(
      "kudu.table" -> args.tableName,
      "kudu.master" -> args.masterAddrs)

    args.operation match {
      case "import" =>
        args.format match {
          case "csv" =>
            val df = sqlContext.read.option("header", args.header).option("delimiter", args.delimiter).csv(args.path)
            kc.upsertRows(df, args.tableName)
          case "parquet" =>
            val df = sqlContext.read.parquet(args.path)
            kc.upsertRows(df, args.tableName)
          case "avro" =>
            val df = sqlContext.read.format("com.databricks.spark.avro").load(args.path)
            kc.upsertRows(df, args.tableName)
          case _ => fail(args.format + s"unknown argument given ")
        }
      case "export" =>
        val df = sqlContext.read.options(kuduOptions).kudu.select(args.columns)
        args.format match {
          case "csv" =>
            df.write.format("com.databricks.spark.csv").option("header", args.header).option("delimiter",
              args.delimiter).save(args.path)
          case "parquet" =>
            df.write.parquet(args.path)
          case "avro" =>
            df.write.format("com.databricks.spark.avro").save(args.path)
          case _ => fail(args.format + s"unknown argument given  ")
        }
      case _ => fail(args.operation + s"unknown argument given ")
    }
  }

  /**
    * Entry point for testing. SparkContext is a singleton,
    * so tests must create and manage their own.
    */
  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(args: Array[String], ss: SparkSession): Unit = {
    run(ArgsCls.parse(args), ss)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Import or Export CSV files from/to Kudu ")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    testMain(args, ss)
  }
}

