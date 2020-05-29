package thoughtworks.wordcount

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Dataset, SparkSession}


object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._

      dataSet.flatMap(line => line
        .toLowerCase()
        .replaceAll("[^a-zA-Z0-9\\s\\']", " ")
        .split(" ")
        .filter(_.nonEmpty)
      )
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._

      dataSet
        .groupBy($"value")
        .agg(count($"value") as "countedWords")
        .orderBy($"value")
    }
  }
}
