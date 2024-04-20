import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, variance}
import scala.collection.mutable

// Define an object CSVReaderApp which will contain the main method to run the program
object Bootstrap2 {
  // The main method, which is the entry point of a Scala program
  def main(args: Array[String]): Unit = {
    // Initialize a Spark session, which is the entry point for Spark functionalities
    val spark = SparkSession.builder()
      .appName("CSV Reader Example") // Name the Spark application
      .config("spark.master", "local") // Run Spark in local mode
      .getOrCreate() // Create the Spark session or get the existing one
    import spark.implicits._

    val filePath = "./ResumeNames.csv"
    // Read the CSV file, with headers and inferred schema
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Create a stratified sample for bootstrapping by taking 25% of the data for each industry without replacement
    // First, we create a map of fractions for each distinct value of 'industry'
    val fractions: Map[String, Double] = df.select("industry").distinct()
      .as[String]
      .collect()
      .map((_ -> 0.25)).toMap

    // Now we apply the stratified sampling
    // Use the seed for reproducibility
    // Use persist for caching
    val sampleData = df.stat.sampleBy("industry", fractions, seed = 42).persist()

    val actualSumStats = mutable.Map[String, (Double, Double)]().withDefaultValue((0.0, 0.0))

    val actualStats = sampleData.groupBy("industry").agg(
      mean("experience").alias("actual_mean_experience"),
      variance("experience").alias("actual_variance_experience")
    ).as[(String, Double, Double)].collect()

    actualStats.foreach { case (industry, mean, variance) =>
      val (sumMean, sumVariance) = actualSumStats(industry)
      actualSumStats(industry) = (sumMean + mean, sumVariance + variance)
    }

    // A mutable map to hold the running sum of means and variances for each category
    val sumStats = mutable.Map[String, (Double, Double)]().withDefaultValue((0.0, 0.0))

    // Perform the bootstrapping 1000 times
    val times = 100
    for (_ <- 1 to times) {
      // Resample with replacement to create the 'resampledData'
      val resampledData = sampleData.sample(withReplacement = true, 1.0)
      // Calculate mean and variance for each category
      val stats = resampledData.groupBy("industry")
        .agg(
          mean("experience").alias("mean_experience"),
          variance("experience").alias("variance_experience")
        ).as[(String, Double, Double)].collect()

      // Add the calculated mean and variance to the running sum in the map
      stats.foreach { case (industry, mean, variance) =>
        val (sumMean, sumVariance) = sumStats(industry)
        sumStats(industry) = (sumMean + mean, sumVariance + variance)
      }
    }

    //
    println("Actual Result:")
    println("Category Mean Variance")
    actualSumStats.foreach { case (industry, (mean, variance)) =>
      println(s"$industry $mean $variance")
    }
    // Calculate and display the average of each quantity by dividing by 1000
    println("Estimate Result:")
    println("Category Mean Variance")
    sumStats.foreach { case (industry, (sumMean, sumVariance)) =>
      val avgMean = sumMean / times
      val avgVariance = sumVariance / times
      println(s"$industry $avgMean $avgVariance")
    }

    // Stop the Spark session
    spark.stop()
  }
}
