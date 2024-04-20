package com.cs522.team2

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable


object CSVReaderApp {


  def resample(data: org.apache.spark.sql.DataFrame, numResamples: Int): Unit = {
    val spark = SparkSession.builder.appName("ResampleData").getOrCreate()
    import spark.implicits._

    val map = mutable.HashMap[String, (Int, Int, Int)]()

    for (i <- 0 until numResamples) {
      println(s"Resampled Data (Iteration ${i + 1}):")
      val dataRDD = data.select("industry", "experience").rdd.
        map(row => (row.getString(0), (row.getInt(1), row.getInt(1) * row.getInt(1))))

      val categorySums = dataRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).collect()

      categorySums.foreach { case (category, (sum, sum_of_squares)) =>
        if (map.contains(category)) {
          val current = map(category)
          val total_sum = current._1 + sum
          val total_count = current._2 + 1
          val total_sum_of_squares = current._3 + sum_of_squares

          map.update(category, (total_sum, total_count, total_sum_of_squares))
        }
        else {
          map.addOne(category, (sum, 1, sum_of_squares))
        }
      }
    }

    println(" Map Count " + map.keys.size)
    map.foreach { case (category, (sum, count, sum_of_squares)) =>
      println(s"Category: $category, Total Sum: $sum, Total Count: $count, Sum of Squares: $sum_of_squares")
    }

    val averagesMap = map.map { case (category, (sum, count, sum_of_squares)) =>
      val mean = sum.toDouble / count
      val mean_of_squares = sum_of_squares.toDouble / count
      val variance = mean_of_squares - mean * mean

      (category, (mean, variance))
    }

    averagesMap.foreach { case (category, (average, variance)) =>
      println(s"Category: $category, Average: $average, Variance: $variance")
    }

    spark.stop()
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Reader Example")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._


    val filePath = "ResumeNames.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val data = df.withColumn("experience", col("experience").cast("integer"))
      .withColumn("industry", col("industry").cast("string"))
    val ratio = 0.002
    val fractionMap = data.select("industry").distinct().collect().map(row => (row.getAs[String]("industry"), ratio)).toMap
    val sampledData = data.stat.sampleBy("industry", fractionMap, seed = 42)

    resample(sampledData, 100)
    spark.stop()
  }
}
