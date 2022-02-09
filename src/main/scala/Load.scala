import org.apache.spark.sql.DataFrame

case class Load(){
  /**
   * Load a transformed file in a remote location. For visualisation purposes, we need only one file so one partition.
   *
   * @param: dataFrame a DataFrame we need to load in a CSV file , dest a String with the name of the location of the destination file.
   */
  def load(dataFrame: DataFrame, dest:String):Unit={
    dataFrame.coalesce(1).write.option("header", "true").csv(dest)
  }
}


