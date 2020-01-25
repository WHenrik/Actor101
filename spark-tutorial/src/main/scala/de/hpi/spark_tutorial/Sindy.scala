package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{collect_set, explode, size, concat_ws, col, asc, desc}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    def makeInclusionList(row: Row): Seq[(String, Seq[String])] = {
      var set = row.getAs[Seq[String]](1)
      return( set.map(ee => (ee, set.filter(_ != ee))) )
    }

    val data = inputs
      .map(ii => spark
      .read
        .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(ii)
    )
      .map(dataframe => dataframe
        .flatMap( rr => rr.schema.fieldNames.map(
          cc => (rr.getAs(cc).toString, cc)
        ) )
      )
      .reduce(_ union _)
      .groupBy($"_1")
      .agg(collect_set('_2).as("databases"))
      .flatMap(makeInclusionList(_))
      .rdd
      .reduceByKey((s1, s2) => s1.intersect(s2))
      .toDF
      .orderBy(asc("_1"))
      .collect()
      .foreach(rr => {
        if (rr.getAs[Seq[String]](1).length > 0) {
          println(s"${rr.getAs(0)} < ${rr.getAs[Seq[String]](1).sorted.mkString(",")}")
        }
      })

  }
}
