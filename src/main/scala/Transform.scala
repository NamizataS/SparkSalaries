import org.apache.spark.sql.functions.{coalesce, col, lower, split, to_timestamp, year}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class Transform(spark:SparkSession){
  val _spark: SparkSession = spark

  /**
   * Drop columns which names are in a list from a DataFrame.
   *
   * @return: a DataFrame which is the same as the first one but with columns dropped
   * @param: dataFrame the DataFrame in which we need to drop columns, columns a List[String] with the name of the columns
   */

  def dropColumns(dataFrame: DataFrame, columns:List[String]):DataFrame={
    dataFrame.drop(columns:_*)
  }

  /**
   * A dataset can contain multiple timestamp format. To avoid any errors while parsing the timestamp
   * we try to match the timestamp to multiple timestamp format using coalesce function. If we want the timestamp to be
   * in the same column as the original one (and so replace the original values), two strings parameters must match
   *
   * @return: a DataFrame with the new column which contains the timestamp
   * @param: dataFrame a DataFrame in which we need to transform string to timestamp, newColumnName a String with the name
   * of the destination column , columnName a String of the original column to transform
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
   * Extract the year from a timestamp. If we want the year to be in the same column as the original one (and so replace the
   * original values), two strings parameters must match
   *
   * @return: a DataFrame with the new column which contains the year.
   * @param: dataFrame a DataFrame in which we need to extract the year from a timestamp,
   * columnName a String of the name of the original column containing a timestamp, newColumnName a String with the name of
   * the destination column.
   */

  def getYear(dataFrame: DataFrame, columnName: String, newColumnName:String):DataFrame={
    dataFrame.withColumn(newColumnName, year(col(columnName)))
  }

  /**
   * Lower all character of strings in a column
   *
   * @return: a DataFrame with the same columns but one of them will have all values lowered.
   * @param: dataFrame a DataFrame in which we need to lower one of the column values, columnName a String with the name of
   * the column whose values we need to lower.
   */
  def lowerString(dataFrame: DataFrame, columnName:String):DataFrame={
    dataFrame.withColumn(columnName, lower(col(columnName)))
  }

  /**
   * Change a column name in a DataFrame
   *
   * @return: a DataFrame with the name of one of the column modified
   * @param: dataFrame a DataFrame in which we need to change the name of one the column, columnName a String representing the
   * name of the column we need to change, newColumnName a String representing the new name we want for the column.
   */

  def changeColumnName(dataFrame: DataFrame, columnName:String, newColumnName:String):DataFrame={
    dataFrame.withColumnRenamed(columnName, newColumnName)
  }

  /**
   * Filter a dataframe given a condition
   *
   * @return: a DataFrame containing the values which meet the condition
   * @param: dataFrame a DataFrame we need to filter, condition a Column we need to apply on dataFrame to keep only the values
   * which meet the condition in it.
   *
   */

  def filterDataset(dataFrame: DataFrame, condition: Column):DataFrame={
    dataFrame.filter(condition)
  }

  /**
   * Split values of a column.  If we want the split array to be in the same column as the original one
   * (and so replace the original values), two strings parameters must match
   *
   * @return: a DataFrame with a column with an array containing all values from a split.
   * @param: dataFrame a DataFrame which contains the column to be split, newColumnName a String with the name of the destination
   * column (result column), columnToSplit a String with the name of the column which columns needs to be splitted, pattern a String
   * with the pattern we need to split the values on the column on.
   */

  def splitColumn(dataFrame: DataFrame, newColumnName:String, columnToSplit:String, pattern:String):DataFrame={
    dataFrame.withColumn(newColumnName, split(col(columnToSplit), pattern))
  }

  /**
   * Get items from an array in a given column
   *
   * @return: a DataFrame with only an item in a column containing an array
   * @param: dataFrame a DataFrame which contains an array column, columnName a String which is the name of the column with the
   * array, index an Int representing the index we want to get the values from.
   */

  def getItems(dataFrame: DataFrame, columnName:String, index:Int):DataFrame={
    dataFrame.withColumn(columnName, col(columnName).getItem(index))
  }

  /**
   * Select columns in dataframe given a list
   *
   * @return: a DataFrame with the selected columns.
   * @param: dataFrame a DataFrame in which we need to select column(s), columns a List[String] with the column(s) we want to select.
   */

  def select_columns(dataFrame: DataFrame, columns:List[String]):DataFrame={
    dataFrame.select(columns.head, columns.tail:_*)
  }

  /**
   * Join two dataframe given a condition
   *
   * @return: a DataFrame which is the result of the join between two dataframes.
   * @param: transformed a (DataFrame, DataFrame) which a tuple of DataFrame and contains the two dataframes we want to join,
   * condition a Column containing the condition on which we are going to join the dataframe
   */

  def joinDataset(transformed:(DataFrame, DataFrame),condition:Column):DataFrame={
    transformed._1.join(transformed._2, condition)
  }
}
