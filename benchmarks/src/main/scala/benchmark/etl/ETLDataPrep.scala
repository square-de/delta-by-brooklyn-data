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

import org.apache.spark.sql.functions.{col,lit}

trait ETLPrepConf extends BenchmarkConf {
  protected def format: Option[String]
  def scaleInGB: Int
  def userDefinedDbName: Option[String]
  def formatName: String = format.getOrElse {
    throw new IllegalArgumentException("format must be specified")
  }
  def dbName: String = userDefinedDbName.getOrElse(s"etlprep_sf${scaleInGB}_${formatName}")
  def dbLocation: String = dbLocation(dbName)
}

case class ETLDataPrepConf(
    protected val format: Option[String] = None,
    scaleInGB: Int = 0,
    userDefinedDbName: Option[String] = None,
    sourcePath: Option[String] = None,
    benchmarkPath: Option[String] = None) extends ETLPrepConf

object ETLDataPrepConf {
  import scopt.OParser
  private val builder = OParser.builder[ETLDataPrepConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("ETL Data Prep"),
      opt[String]("format")
        .required()
        .action((x, c) => c.copy(format = Some(x)))
        .text("file format to use"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the TPCDS benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud storage path to be used for creating table and generating reports"),
      opt[String]("source-path")
        .optional()
        .valueName("<path to the TPC-DS raw input data>")
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("The location of the TPC-DS raw input data"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(userDefinedDbName = Some(x)))
        .text("Name of the target database to create with ETL Prep tables in necessary format"),
    )
  }

  def parse(args: Array[String]): Option[ETLDataPrepConf] = {
    OParser.parse(argParser, args, ETLDataPrepConf())
  }
}

class ETLDataPrep(conf: ETLDataPrepConf) extends Benchmark(conf) {
  import ETLDataPrep._

  val dbName = conf.dbName
  val dbLocation = conf.dbLocation(dbName, suffix=benchmarkId.replace("-", "_"))
  val sourceFormat = "parquet"
  val formatName = conf.formatName

  require(conf.scaleInGB > 0)
  require(Seq(1, 1000, 3000).contains(conf.scaleInGB), "")
  val sourceLocation = conf.sourcePath.getOrElse {
    s"s3://your-default-bucket/path-to-parquet/tpcds_sf1000_parquet/"
  }
  val extraConfs: Map[String, String] = Map(
    "spark.sql.broadcastTimeout" -> "7200",
    "spark.sql.crossJoin.enabled" -> "true"
  )

  val etlQueries = new ETLQueries(dbLocation, formatName, sourceLocation, sourceFormat, "")
  val prepQueries: Map[String, String] = etlQueries.prepQueries

  def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("WARN")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))

    runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", s"etlPrep0.1-drop-database")
    runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName}", s"etlPrep0.2-create-database")
    runQuery(s"USE $dbName", s"etlPrep0.3-use-database")

    // Iterate over ETL Prep queries in order
    prepQueries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
      runQuery(sql, iteration = Some(1), queryName = name)
    }

    // Gather stats for ETL Prep tables to check size and rows
    var tbl_list = spark.sql("SHOW TABLES")
    for (tbl <- tbl_list.select("tableName").collect()) {
      val tbl_name: String = tbl.mkString("\n")
      spark.sql(s"ANALYZE TABLE ${tbl_name} COMPUTE STATISTICS")
      spark.sql(s"DESCRIBE EXTENDED ${tbl_name}")
        .filter(col("col_name") === "Statistics").select("data_type")
        .withColumn("tbl_name",lit(s"${tbl_name}")).show(false)
    }
  }
}

object ETLDataPrep {
  def main(args: Array[String]): Unit = {
    ETLDataPrepConf.parse(args).foreach { conf =>
      new ETLDataPrep(conf).run()
    }
  }
}
