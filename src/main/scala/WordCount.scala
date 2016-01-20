import java.security.Timestamp

import org.apache.spark.{
  SparkContext,
  SparkConf
}

object WordCount {
  def main(args: Array[String]) {

    println("Hello World! This is Spark WordCount example in Scala.")

    val sparkConf = new SparkConf()
      .setAppName("SparkWordCountScala")
      .setMaster("local[2]")

    val sparkContext = new SparkContext(sparkConf)

    // val data = "This is some text to use for word count example  My name is Vinay This is spark example"
    val input = sparkContext.textFile("inputFile") //  String RDD
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1))
    val wordCounts = counts.reduceByKey((x, y) => x+y) // PairRDD function
    wordCounts.saveAsTextFile("target/output"+(System.currentTimeMillis/1000).toString)

    wordCounts.collect().foreach(println)
    sparkContext.stop()
  }
}