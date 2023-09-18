/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

trait ETLConf extends BenchmarkConf {
  protected def format: Option[String]
  def scaleInGB: Int
  def userDefinedDbName: Option[String]
  def formatName: String = format.getOrElse {
    throw new IllegalArgumentException("format must be specified")
  }
  def dbName: String = userDefinedDbName.getOrElse(s"etl_sf${scaleInGB}_${formatName}")
  def dbLocation: String = dbLocation(dbName)
  def customWriteMode: Option[String]
  def writeMode: String = customWriteMode.getOrElse("copy-on-write")
  def experiment: Option[String]
}

case class ETLBenchmarkConf(
     protected val format: Option[String] = None,
     scaleInGB: Int = 0,
     userDefinedDbName: Option[String] = None,
     iterations: Int = 3,
     benchmarkPath: Option[String] = None,
     sourcePath: Option[String] = None,
     customWriteMode: Option[String] = None,
     experiment: Option[String] = None) extends ETLConf

object ETLBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[ETLBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("ETL Benchmark"),
      opt[String]("format")
        .required()
        .action((x, c) => c.copy(format = Some(x)))
        .text("Spark's short name for the file format to use"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the ETL benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("source-path")
        .optional()
        .valueName("<path to the TPC-DS raw input data>")
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("The location of the TPC-DS raw input data"),
      opt[String]("iterations")
        .optional()
        .valueName("<number of iterations>")
        .action((x, c) => c.copy(iterations = x.toInt))
        .text("Number of times to run the queries"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(userDefinedDbName = Some(x)))
        .text("Name of the target database to create with ETL Prep tables in necessary format"),
      opt[String]("write_mode")
        .optional()
        .valueName("<copy-on-write or merge-on-read>")
        .action((x, c) => c.copy(customWriteMode = Some(x)))
        .text("Strategy used for writing table changes. `copy-on-write` [default] or `merge-on-read`"),
      opt[String]("experiment")
        .optional()
        .valueName("<compaction or zorder or vacuum or all>")
        .action((x, c) => c.copy(experiment = Some(x)))
        .text("Task type for the benchmark. Use one of the following options: compaction, zorder, vacuum, all. Use None for controlling the task (no specific experiment) [default]"),
    )
  }

  def parse(args: Array[String]): Option[ETLBenchmarkConf] = {
    OParser.parse(argParser, args, ETLBenchmarkConf())
  }
}

class ETLBenchmark(conf: ETLBenchmarkConf) extends Benchmark(conf) {
  val dbName = conf.dbName
  val dbLocation = conf.dbLocation(dbName, suffix=benchmarkId.replace("-", "_"))
  val sourceFormat = "parquet"
  val formatName = conf.formatName
  val writeMode = conf.writeMode
  val experiment = conf.experiment

  val tblProperties = formatName match {
    case "iceberg" =>
      s"""TBLPROPERTIES ('format-version'='2',
                       'write.delete.mode'='${writeMode}',
                       'write.update.mode'='${writeMode}',
                       'write.merge.mode'='${writeMode}')"""
    case "hudi" =>
      // NOTE: This is only used to create single (denormalized) store_sales table;
      //       as such we're reusing primary key we're generally using for store_sales in TPC-DS benchmarks
      s"""TBLPROPERTIES (
         |  type = '${if (writeMode == "copy-on-write") "cow" else "mor"}',
         |  primaryKey = 'ss_item_sk,ss_ticket_number',
         |  preCombineField = 'ss_sold_time_sk',
         |  'hoodie.table.name' = 'store_sales_denorm_${formatName}',
         |  'hoodie.table.partition.fields' = 'ss_sold_date_sk',
         |  'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.ComplexKeyGenerator',
         |  'hoodie.parquet.compression.codec' = 'snappy',
         |  'hoodie.datasource.write.hive_style_partitioning' = 'true',
         |  'hoodie.sql.insert.mode'= 'non-strict',
         |  'hoodie.sql.bulk.insert.enable' = 'true',
         |  'hoodie.combine.before.insert' = 'false'
         |)""".stripMargin

    case "delta" => ""
  }

  require(conf.scaleInGB > 0)
  require(Seq(1, 1000, 3000).contains(conf.scaleInGB), "")
  val sourceLocation = conf.sourcePath.getOrElse {
      s"s3://your-default-bucket/path-to-parquet/etl_sf${conf.scaleInGB}_parquet/"
    }
  val extraConfs: Map[String, String] = Map(
    "spark.sql.broadcastTimeout" -> "7200",
    "spark.sql.crossJoin.enabled" -> "true"
  )

  val etlQueries = new ETLQueries(dbLocation, formatName, sourceLocation, sourceFormat, tblProperties)
  val writeQueries: Map[String, String] = etlQueries.writeQueries
  val readQueries: Map[String, String] = etlQueries.readQueries
  val compactionWriteQueries: Map[String, String] = etlQueries.compactionWriteQueries
  val zorderWriteQueries: Map[String, String] = etlQueries.zorderWriteQueries
  val vacuumWriteQueries: Map[String, String] = etlQueries.vacuumWriteQueries

  // TODO: 根據參數決定 batch job 的實驗項目 (origin/compaction/zorder/vacuum/all) 準備對應的 writeQueries
  // 請根據實驗項目 experiment 的不同，將對應的 writeQueries (compactionWriteQueries|zorderWriteQueries|vacuumWriteQueries)結合 writeQueries 指派給 batchJobWriteQueries
  val batchJobWriteQueries = experiment match {
    case Some("compaction") => writeQueries ++ compactionWriteQueries
    case Some("zorder") => writeQueries ++ zorderWriteQueries
    case Some("vacuum") => writeQueries ++ vacuumWriteQueries
    case Some("all") => writeQueries ++ compactionWriteQueries ++ zorderWriteQueries ++ vacuumWriteQueries
    case _ => writeQueries
    }

  def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("WARN")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))

    log("square log ===> experiment: " + experiment)

    runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", s"etl0.1-drop-database")
    runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName}", s"etl0.2-create-database")
    runQuery(s"USE $dbName", s"etl0.3-use-database")
    runQuery(s"DROP TABLE IF EXISTS store_sales_denorm_${formatName}", s"etl0.4-drop-table")


    batchJobWriteQueries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
      runQuery(sql, iteration = Some(1), queryName = name)
      // Print table stats
      if (conf.formatName == "iceberg") {
        runQuery(s"SELECT * FROM spark_catalog.${dbName}.store_sales_denorm_${formatName}.snapshots",
          printRows = true, queryName = s"${name}-file-stats")
      } else if (conf.formatName == "delta") {
        runQuery(s"DESCRIBE HISTORY store_sales_denorm_${formatName}",
          printRows = true, queryName = s"${name}-file-stats")
      }

      // TODO: 只有在 writeQueries 是 "etl6-deleteGdpr" 才需要進行查詢測試
      val needRunReadQueryName = List("etl6-deleteGdpr", "etl7-compaction", "etl8-zorder", "etl9-vacuum")
      if (needRunReadQueryName.contains(name)) {
            // Run read queries
          for (iteration <- 1 to conf.iterations) {
            readQueries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
              runQuery(sql, iteration = Some(iteration), queryName = name)
            }
          }
      } else {
          log(s"Skipping executing read queries for ${name}")
      }



    }
    val results = getQueryResults().filter(_.name.startsWith("q"))
    if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty) ) {
      val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
        log(s"Queries Completed. Checking size: ${results.length}, actual:${results.size} expected: ${conf.iterations}")

        assert(results.size >= conf.iterations)
        log(results.map(_.durationMs.get).sorted.mkString(","))

        val medianMs = results.map(_.durationMs.get).sorted
            .drop(math.floor(results.size / 2.0).toInt).head

        log(s"${q}'s medianMs: ${medianMs}")

        (q, medianMs / 1000.0)

      }
      val sumOfMedians = medianDurationSecPerQuery.map(_._2).sum
      reportExtraMetric("ETL-result-seconds", sumOfMedians)
    }
  }
}

object ETLBenchmark {
  def main(args: Array[String]): Unit = {
    ETLBenchmarkConf.parse(args).foreach { conf =>
      new ETLBenchmark(conf).run()
    }
  }
}

