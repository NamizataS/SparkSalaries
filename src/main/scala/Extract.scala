import org.apache.spark.sql.{DataFrame, SparkSession}

case class Extract(spark:SparkSession){
  val _spark:SparkSession = spark
  this._spark.sparkContext.setLogLevel("ERROR")

  /**
   * Reading a CSV file. It is a structured file so we use the DataFrame API.
   *
   * @return: DataFrame
   * @param: filename
   */
  def extract(filename:String): DataFrame ={
    this._spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    this._spark.read.option("inferSchema", "true").option("header", "true").csv(filename)
  }
}
