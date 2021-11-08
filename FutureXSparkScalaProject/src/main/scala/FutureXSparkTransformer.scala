import java.util.Properties
import common.{PostgresCommon, SparkCommon}
import org.apache.log4j.spi.LoggerFactory

import org.apache.spark.sql.SparkSession

object FutureXSparkTransformer {

  //Adding some more professional looking for the project logging
  //private val Logger = LoggerFactory.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {
    
      //Logger.INFO("main method started")
      //Logger.WARN("This is warning")

      val spark : SparkSession = SparkCommon.createSparkSession()

      //Create a DataFrame from Postgres Course Catalog table
      println("Creating Dataframe from Postgres")

      val pgTable = "futurexschema.futurex_course_catalog"
      //server:port/database_name
      val pgCourseDataframe = PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable)
      println("Fetched")

      pgCourseDataframe.show()
      println("Shown")



  }
}
