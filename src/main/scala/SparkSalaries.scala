import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, split}

object SparkSalaries extends App {
  ETL().etl()
}

