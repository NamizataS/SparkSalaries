import org.apache.spark.sql.functions.{coalesce, col, lower, split, to_timestamp, year}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

case class Transform(spark:SparkSession){
  val _spark: SparkSession = spark

  /**
   * Drop columns which names are in a list from a DataFrame.
   *
   * @return: DataFrame
   * @param: DataFrame, List[String]
   */

  def dropColumns(dataFrame: DataFrame, columns:List[String]):DataFrame={
    dataFrame.drop(columns:_*)
  }

  /**
   * A dataset can contain multiple timestamp format. To avoid any errors while parsing the timestamp
   * we try to match the timestamp to multiple timestamp format using coalesce function.
   *
   * @return: DataFrame
   * @param: DataFrame, String, String
   */

  def stringToTimestamp(dataFrame: DataFrame, newcolumnName: String, columnName:String):DataFrame={
    dataFrame.withColumn(newcolumnName, coalesce(
      to_timestamp(col(columnName), "M/d/yyyy HH:mm:ss"),
      to_timestamp(col(columnName), "M/dd/yyyy HH:mm:ss"),
      to_timestamp(col(columnName), "MM/d/yyyy HH:mm:ss"),
      to_timestamp(col(columnName), "MM/dd/yyyy HH:mm:ss"),
      to_timestamp(col(columnName), "M/dd/yyyy H:mm:ss"),
      to_timestamp(col(columnName), "M/d/yyyy H:mm:ss")
    ))
  }

  /**
   * Extract the year from a timestamp.
   *
   * @return: DataFrame
   * @param: DataFrame, String, String
   */

  def getYear(dataFrame: DataFrame, columnName: String, newColumnName:String):DataFrame={
    dataFrame.withColumn(newColumnName, year(col(columnName)))
  }

  /**
   * Lower all character of strings in a column
   *
   * @return: DataFrame
   * @param: DataFrame, String
   */
  def lowerString(dataFrame: DataFrame, columnName:String):DataFrame={
    dataFrame.withColumn(columnName, lower(col(columnName)))
  }

  def changeColumnName(dataFrame: DataFrame, columnName:String, newColumnName:String):DataFrame={
    dataFrame.withColumnRenamed(columnName, newColumnName)
  }

  def filterDataset(dataFrame: DataFrame, condition: Column):DataFrame={
    dataFrame.filter(condition)
  }

  def splitColumn(dataFrame: DataFrame, newColumnName:String, columnToSplit:String, pattern:String):DataFrame={
    dataFrame.withColumn(newColumnName, split(col(columnToSplit), pattern))
  }

  def getItems(dataFrame: DataFrame, columnName:String, index:Int):DataFrame={
    dataFrame.withColumn(columnName, col(columnName).getItem(index))
  }

  def select_columns(dataFrame: DataFrame, columns:List[String]):DataFrame={
    dataFrame.select(columns.head, columns.tail:_*)
  }

  def joinDataset(transformed:(DataFrame, DataFrame),condition:Column):DataFrame={
    transformed._1.join(transformed._2, condition)
  }
}
