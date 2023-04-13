package rizwan.minhas

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error("Usage HelloSpark filename")
      System.exit(1)
    }

    logger.info("starting HelloSpark")
    val spark = SparkSession.builder().config(getSparkAppConfig).getOrCreate()
    val surveyDF = loadSurveyDataFrame(spark, args(0))
    val partitionSurveyDF = surveyDF.repartition(2)
    val countDF = countByCountry(partitionSurveyDF)
    countDF.foreach(row => logger.info(s"Country: ${row.getString(0)} Count: ${row.getLong(1)}"))

    logger.info(countDF.collect().mkString("-->"))

    logger.info("Finished HelloSpark")
    spark.stop()
  }

  def countByCountry(df: DataFrame): DataFrame = {
    df.where("Age < 40")
      .select("Age", "Gender", "Country", "State")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDataFrame(sparkSession: SparkSession, dataFile: String): DataFrame = {
    sparkSession.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }

  def getSparkAppConfig: SparkConf = {
    val sparkAppConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader)
    props.forEach((k,v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }
}
