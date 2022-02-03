import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ETL(){
  val spark:SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  this.spark.sparkContext.setLogLevel("ERROR")

  def extract_salaries():DataFrame={
    val salaries_file = "./datas/Levels_Fyi_Salary_Data.csv"
    Extract(this.spark).extract(salaries_file)
  }

  def extract_revenues():DataFrame={
    val revenues_file = "./datas/fortune500.csv"
    Extract(this.spark).extract(revenues_file)
  }

  def extract_cities():DataFrame={
    val cities_file = "./datas/worldcities.csv"
    Extract(this.spark).extract(cities_file)
  }

  def transform_revenues(df_revenues:DataFrame):DataFrame={
    val transform = Transform(this.spark)
    val revenues = transform.lowerString(df_revenues, "Name")
    val top_companies = transform.filterDataset(revenues, col("Rank") <= 5)
    transform.changeColumnName(top_companies, "Year", "date")
  }

  def transform_salaries(df_salaries:DataFrame, df_transformed_revenues:DataFrame):DataFrame={
    val transform = Transform(this.spark)
    val columnsToDrop = List("tag", "stockgrantvalue", "gender", "otherdetails", "cityid", "dmaid",
      "rowNumber", "Some_College", "Race_Asian", "Race_White", "Race_Two_Or_More", "Race_Black",
      "Race_Hispanic", "Race", "Masters_Degree", "Bachelors_Degree", "Doctorate_Degree", "Highschool")
    var salaries = transform.dropColumns(df_salaries, columnsToDrop)
    salaries = transform.stringToTimestamp(salaries, "timestamp", "timestamp")
    salaries = transform.getYear(salaries, "timestamp", "year")
    var top_salaries_companies = transform.joinDataset((salaries, df_transformed_revenues), salaries("company") <=> df_transformed_revenues("Name") &&
      salaries("year") <=> df_transformed_revenues("date"))
    transform.dropColumns(top_salaries_companies, List("date", "Name"))
  }

  def transform_cities(df_cities:DataFrame, df_transformed_salaries:DataFrame):DataFrame={
    val transform = Transform(this.spark)
    var locations = transform.select_columns(df_transformed_salaries, List("location"))
    locations = transform.splitColumn(locations, "City", "location", ",")
    locations = transform.getItems(locations, "City", 0)
    val joinedCities = transform.joinDataset((locations, df_cities), locations("City") <=> df_cities("city_ascii"))
    val columnsToKeep = List("location", "city_ascii", "lat", "lng")
    transform.select_columns(joinedCities, columnsToKeep)
  }


  def etl(): Unit ={
    //Extract
    var salaries = this.extract_salaries()
    var revenues = this.extract_revenues()
    var cities = this.extract_cities()
    //Transform
    val transformedRevenues = this.transform_revenues(revenues)
    val transformedSalaries = this.transform_salaries(salaries, transformedRevenues)
    val transformedCities = this.transform_cities(cities, transformedSalaries)
    //Load
    val loader = Load()
    loader.load(transformedSalaries, "filtered_salaries")
    loader.load(transformedRevenues, "filtered_revenues")
    loader.load(transformedCities, "filteredCities")
  }
}