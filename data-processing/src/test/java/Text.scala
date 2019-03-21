import org.apache.spark.{SparkConf, SparkContext}

object Text {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val text = sparkContext.textFile("1.txt")

    text.foreach(println(_))

  }

}
