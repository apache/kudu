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

import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{KuduClient, KuduSession, KuduTable}
import org.apache.kudu.mapreduce.tools.BigLinkedListCommon.{Xoroshiro128PlusRandom, _}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.yetus.audience.InterfaceAudience
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * Spark port of [[org.apache.kudu.mapreduce.tools.IntegrationTestBigLinkedList]].
  *
  * Major differences:
  *   * Currently, only the generator and verifier jobs are implemented.
  *   * The heads table is not written to during generate, and not used during verify.
  *   * The generate job does not write in batches. Instead, it writes a head node,
  *     followed by many tail nodes into the table, and then updates just the
  *     head node to point at the final tail node. Writes use AUTO_FLUSH_BACKGROUND.
  *     This is hopefully easier to understand, and has the advantage of stressing
  *     slightly different code paths than the MR version.
  */
object IntegrationTestBigLinkedList {
  val LOG: Logger = LoggerFactory.getLogger(IntegrationTestBigLinkedList.getClass)

  def usage: String =
    s"""
       | Usage: COMMAND [COMMAND options]");
       |    where COMMAND is one of:
       |
       |        generate    A Spark job that generates linked list data.
       |
       |        verify      A Spark job that verifies generated linked list data.
       |                    Fails the job if any UNDEFINED, UNREFERENCED, or
       |                    EXTRAREFERENCES nodes are found. Do not run at the
       |                     same time as the Generate command.
       |
       |        loop        Loops the generate and verify jobs indefinitely.
       |                    Data is not cleaned between runs, so each iteration
       |                    adds more data.
    """.stripMargin

  def parseIntFlag(flag: String, num: String): Int = {
    Try(num.toInt).getOrElse(fail(s"failed to parse $flag value as integer: $num"))
  }

  def parseLongFlag(flag: String, num: String): Long = {
    Try(num.toLong).getOrElse(fail(s"failed to parse $flag value as integer: $num"))
  }

  def fail(msg: String): Nothing = {
    System.err.println(msg)
    sys.exit(1)
  }

  def nanosToHuman(n: Long): String = {
    if (n > 10 * 60 * 1e9) "%s.3m".format(n / (60 * 1e9))
    else if (n > 1e9) "%s.3s".format(n / 1e9)
    else if (n > 1e6) "%s.3ms".format(n / 1e6)
    else if (n > 1e3) "%s.3μs".format(n / 1e3)
    else s"${n}ns"
  }

  def defaultMasterAddrs: String = InetAddress.getLocalHost.getCanonicalHostName

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) { fail(usage) }

    args(0).toLowerCase() match {
      case "generate" => Generator.main(args.slice(1, args.length))
      case "verify" => Verifier.main(args.slice(1, args.length))
      case "loop" => Looper.main(args.slice(1, args.length))
      case _ => fail(usage)
    }
  }
}

object Generator {
  import IntegrationTestBigLinkedList.{LOG, defaultMasterAddrs, fail, nanosToHuman, parseIntFlag}

  def usage: String =
    s"""
       | Usage: generate --tasks=<tasks> --lists=<lists> --nodes=<nodes>
       |                 --hash-partitions=<hash-partitions> --range-partitions=<range-partitions>
       |                 --replicas=<replicas> --master-addrs=<master-addrs> --table-name=<table-name>
       |    where
       |      tasks: number of Spark tasks to create, default: 1
       |      lists: number of linked lists to create per task, default: 1
       |      nodes: number of nodes to create per list, default: 10000000
       |      hashPartitions: number of hash partitions to create for the new linked list table, if it doesn't exist, default: 1
       |      rangePartitions: number of range partitions to create for the new linked list table, if it doesn't exist, default: 1
       |      replicas: number of replicas to create for the new linked list table, if it doesn't exist, default: 1
       |      master-addrs: comma separated addresses of Kudu master nodes, default: $defaultMasterAddrs
       |      table-name: the name of the linked list table, default: $DEFAULT_TABLE_NAME
     """.stripMargin

  case class Args(tasks: Int = 1,
                  lists: Int = 1,
                  nodes: Int = 10000000,
                  hashPartitions: Int = 1,
                  rangePartitions: Int = 1,
                  replicas: Int = 1,
                  masterAddrs: String = defaultMasterAddrs,
                  tableName: String = DEFAULT_TABLE_NAME)

  object Args {
    private def parseInner(options: Args, args: List[String]): Args = {
      args match {
        case Nil => options
        case "--help" :: _ =>
          System.err.println(usage)
          sys.exit(0)
        case flag :: Nil => fail(s"flag $flag has no value\n$usage")
        case flag :: value :: tail =>
          val newOptions: Args = flag match {
            case "--tasks" => options.copy(tasks = parseIntFlag(flag, value))
            case "--lists" => options.copy(lists = parseIntFlag(flag, value))
            case "--nodes" => options.copy(nodes = parseIntFlag(flag, value))
            case "--hash-partitions" => options.copy(hashPartitions = parseIntFlag(flag, value))
            case "--range-partitions" => options.copy(rangePartitions = parseIntFlag(flag, value))
            case "--replicas" => options.copy(replicas = parseIntFlag(flag, value))
            case "--master-addrs" => options.copy(masterAddrs = value)
            case "--table-name" => options.copy(tableName = value)
            case _ => fail(s"unknown generate flag $flag")
          }
          parseInner(newOptions, tail)
      }
    }

    def parse(args: Array[String]): Args = {
      parseInner(Args(), args.flatMap(_.split('=')).toList)
    }
  }

  def run(args: Args, ss: SparkSession): Unit = {
    val kc = new KuduContext(args.masterAddrs, ss.sparkContext)
    val applicationId = ss.sparkContext.applicationId

    val client: KuduClient = kc.syncClient
    if (!client.tableExists(args.tableName)) {
      val schema = getTableSchema
      val options = getCreateTableOptions(schema, args.replicas,
        args.rangePartitions, args.hashPartitions)
      client.createTable(args.tableName, getTableSchema, options)
    }

    // Run the generate tasks
    ss.sparkContext.makeRDD(0 until args.tasks, args.tasks)
      .foreach(_ => generate(args, applicationId, kc))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Integration Test Big Linked List Generator")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    run(Args.parse(args), ss)
  }

  /**
    * Entry point for testing. SparkContext is a singleton,
    * so tests must create and manage their own.
    */
  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(args: Array[String], ss: SparkSession): Unit = {
    run(Args.parse(args), ss)
  }

  def generate(args: Args,
               applicationId: String,
               kc: KuduContext): Unit = {
    val taskContext = TaskContext.get()
    val clientId = s"$applicationId-${taskContext.partitionId()}"

    val rand = new Xoroshiro128PlusRandom()

    val client:KuduClient = kc.syncClient

    val table: KuduTable = client.openTable(args.tableName)
    val session: KuduSession = client.newSession()
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    try {
      for (_ <- 0 until args.lists) {
        val start = System.nanoTime()
        insertList(clientId, args, table, session, rand)
        LOG.info(s"$clientId inserted ${args.nodes} node linked list in {}",
                 nanosToHuman(System.nanoTime() - start))
      }
    } finally {
      session.close()
    }
  }

  def insertList(clientId: String,
                 args: Args,
                 table: KuduTable,
                 session: KuduSession,
                 rand: Xoroshiro128PlusRandom): Unit = {

    // Write the head node to the table.
    val headKeyOne = rand.nextLong()
    val headKeyTwo = rand.nextLong()

    {
      val insert = table.newInsert()
      insert.getRow.addLong(COLUMN_KEY_ONE_IDX, headKeyOne)
      insert.getRow.addLong(COLUMN_KEY_TWO_IDX, headKeyTwo)
      insert.getRow.addLong(COLUMN_ROW_ID_IDX, 0)
      insert.getRow.addString(COLUMN_CLIENT_IDX, clientId)
      insert.getRow.addInt(COLUMN_UPDATE_COUNT_IDX, 0)
      session.apply(insert)
    }

    // Write the rest of the list nodes.
    var prevKeyOne = headKeyOne
    var prevKeyTwo = headKeyTwo
    for (rowIdx <- 1 until args.nodes) {
      val keyOne = rand.nextLong()
      val keyTwo = rand.nextLong()
      val insert = table.newInsert()
      insert.getRow.addLong(COLUMN_KEY_ONE_IDX, keyOne)
      insert.getRow.addLong(COLUMN_KEY_TWO_IDX, keyTwo)
      insert.getRow.addLong(COLUMN_PREV_ONE_IDX, prevKeyOne)
      insert.getRow.addLong(COLUMN_PREV_TWO_IDX, prevKeyTwo)
      insert.getRow.addLong(COLUMN_ROW_ID_IDX, rowIdx)
      insert.getRow.addString(COLUMN_CLIENT_IDX, clientId)
      insert.getRow.addInt(COLUMN_UPDATE_COUNT_IDX, 0)
      session.apply(insert)
      prevKeyOne = keyOne
      prevKeyTwo = keyTwo
    }

    // Update the head node's previous pointers to point to the last node.
    {
      val update = table.newUpdate()
      update.getRow.addLong(COLUMN_KEY_ONE_IDX, headKeyOne)
      update.getRow.addLong(COLUMN_KEY_TWO_IDX, headKeyTwo)
      update.getRow.addLong(COLUMN_PREV_ONE_IDX, prevKeyOne)
      update.getRow.addLong(COLUMN_PREV_TWO_IDX, prevKeyTwo)
      session.apply(update)
    }

    session.flush()
    val errors = session.getPendingErrors
    if (errors.getRowErrors.length > 0) {
      throw new RuntimeException(errors.getRowErrors
        .map(_.getErrorStatus.toString)
        .mkString("Row errors: [", ", ", "]"))
    }
  }
}

object Verifier {
  import IntegrationTestBigLinkedList.{defaultMasterAddrs, fail, parseLongFlag}

  def usage: String =
    s"""
       | Usage: verify --nodes=<nodes> --master-addrs=<master-addrs> --table-name=<table-name>
       |    where
       |      nodes: number of nodes expected to be in the linked list table
       |      master-addrs: comma separated addresses of Kudu master nodes, default: $defaultMasterAddrs
       |      table-name: the name of the linked list table, default: $DEFAULT_TABLE_NAME
     """.stripMargin

  case class Args(nodes: Option[Long] = None,
                  masterAddrs: String = defaultMasterAddrs,
                  tableName: String = DEFAULT_TABLE_NAME)

  object Args {
    private def parseInner(options: Args, args: List[String]): Args = {
      args match {
        case Nil => options
        case "--help" :: _ =>
          System.err.println(usage)
          sys.exit(0)
        case flag :: Nil => fail(s"flag $flag has no value\n$usage")
        case flag :: value :: tail =>
          val newOptions = flag match {
            case "--nodes" => options.copy(nodes = Some(parseLongFlag(flag, value)))
            case "--master-addrs" => options.copy(masterAddrs = value)
            case "--table-name" => options.copy(tableName = value)
            case _ => fail(s"unknown verify flag $flag")
          }
          parseInner(newOptions, tail)
      }
    }

    def parse(args: Array[String]): Args = {
      parseInner(Args(), args.flatMap(_.split('=')).toList)
    }
  }

  case class Counts(referenced: Long,
                    unreferenced: Long,
                    extrareferences: Long,
                    undefined: Long)

  /**
    * Verifies the expected count against the count of nodes from a verification run.
    * @param expected the expected node count
    * @param counts the node counts returned by the verification job
    * @return an error message, if the verification fails
    */
  def verify(expected: Option[Long], counts: Counts): Option[String] = {
    if (expected.exists(_ != counts.referenced)) {
      Some(s"Found ${counts.referenced} referenced nodes, " +
           s"which does not match the expected count of ${expected.get} nodes")
    } else if (counts.unreferenced > 0) {
      Some(s"Found ${counts.unreferenced} unreferenced nodes")
    } else if (counts.undefined > 0) {
      Some(s"Found ${counts.undefined} undefined nodes")
    } else if (counts.extrareferences > 0) {
      Some(s"Found ${counts.extrareferences} extra-referenced nodes")
    } else None
  }

  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def run(args: Args, ss: SparkSession): Counts = {
    import org.apache.kudu.spark.kudu._
    val sql = ss.sqlContext

    sql.read
       .option("kudu.master", args.masterAddrs)
       .option("kudu.table", args.tableName)
       .kudu
       .createOrReplaceTempView("nodes")

    // Get a table of all nodes and their ref count
    sql.sql(
      s"""
         | SELECT (SELECT COUNT(*)
         |         FROM nodes t2
         |         WHERE t1.$COLUMN_KEY_ONE = t2.$COLUMN_PREV_ONE
         |           AND t1.$COLUMN_KEY_TWO = t2.$COLUMN_PREV_TWO) AS ref_count
         | FROM nodes t1
     """.stripMargin).createOrReplaceTempView("ref_counts")

    // Compress the ref counts down to 0, 1, or 2.  0 Indicates no references,
    // 1 indicates a single reference, and 2 indicates more than 1 reference.
    sql.sql(
      s"""
         | SELECT (CASE WHEN ref_count > 1 THEN 2 ELSE ref_count END) as ref_count
         | FROM ref_counts
       """.stripMargin).createOrReplaceTempView("ref_counts")

    // Aggregate the ref counts
    sql.sql(
      s"""
         | SELECT ref_count, COUNT(*) as nodes
         | FROM ref_counts
         | GROUP BY ref_count
       """.stripMargin).createOrReplaceTempView("ref_counts")

    // Transform the ref count to a state.
    sql.sql(
      s"""
         | SELECT CASE WHEN ref_count = 0 THEN "UNREFERENCED"
         |             WHEN ref_count = 1 THEN "REFERENCED"
         |             ELSE "EXTRAREFERENCES" END as state,
         |        nodes
         | FROM ref_counts
       """.stripMargin).createOrReplaceTempView("ref_counts")

    // Find all referenced but undefined nodes.
    sql.sql(
      s"""
         | SELECT $COLUMN_CLIENT as list, "UNDEFINED" as state, COUNT(*) as nodes
         | FROM nodes t1
         | WHERE $COLUMN_PREV_ONE IS NOT NULL
         |   AND $COLUMN_PREV_TWO IS NOT NULL
         |   AND NOT EXISTS (
         |       SELECT * FROM nodes t2
         |       WHERE t1.$COLUMN_PREV_ONE = t2.$COLUMN_KEY_ONE
         |         AND t1.$COLUMN_PREV_TWO = t2.$COLUMN_KEY_TWO)
         | GROUP BY $COLUMN_CLIENT
       """.stripMargin).createOrReplaceTempView("undefined")

    // Combine the ref counts and undefined counts tables.
    val rows = sql.sql(
      s"""
         | SELECT state, nodes FROM ref_counts
         | UNION ALL
         | SELECT state, nodes FROM undefined
       """.stripMargin).collect()

    // Extract the node counts for each state from the rows.
    rows.foldLeft(Counts(0, 0, 0, 0))((counts, row) => {
      val state = row.getString(0)
      val count = row.getLong(1)
      state match {
        case "REFERENCED" => counts.copy(referenced = count)
        case "UNREFERENCED" => counts.copy(unreferenced = count)
        case "UNDEFINED" => counts.copy(undefined = count)
        case "EXTRAREFERENCES" => counts.copy(extrareferences = count)
      }
    })
  }

  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(arguments: Array[String], ss: SparkSession): Counts = {
    run(Args.parse(arguments), ss)
  }

  def main(arguments: Array[String]): Unit = {
    val args = Args.parse(arguments)
    val conf = new SparkConf().setAppName("Integration Test Big Linked List Generator")
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val counts = run(Args.parse(arguments), ss)
    verify(args.nodes, counts).map(fail)
  }
}

object Looper {
  import IntegrationTestBigLinkedList.{LOG, fail}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Integration Test Big Linked List Looper")
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val genArgs = Generator.Args.parse(args)
    var verifyArgs = Verifier.Args(masterAddrs = genArgs.masterAddrs,
                                   tableName = genArgs.tableName)
    val nodesPerLoop = genArgs.tasks * genArgs.lists * genArgs.nodes

    for (n <- Stream.from(1)) {
      Generator.run(genArgs, ss)
      val count = Verifier.run(verifyArgs, ss)
      val expected = verifyArgs.nodes.map(_ + nodesPerLoop)
      Verifier.verify(expected, count).map(fail)
      verifyArgs = verifyArgs.copy(nodes = Some(expected.getOrElse(nodesPerLoop)))
      LOG.info("*************************************************")
      LOG.info(s"Completed $n loops. Nodes verified: ${count.referenced}")
      LOG.info("*************************************************")
    }
  }
}
