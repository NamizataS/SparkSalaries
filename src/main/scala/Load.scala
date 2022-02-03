import org.apache.spark.sql.{DataFrame, SparkSession}

case class Load(){

  def load(dataFrame: DataFrame, dest:String):Unit={
    dataFrame.repartition(1).write.option("header", "true").csv(dest)
  }
}


