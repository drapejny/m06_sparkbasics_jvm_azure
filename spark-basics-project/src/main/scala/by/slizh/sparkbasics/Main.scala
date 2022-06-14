package by.slizh.sparkbasics

import by.slizh.sparkbasics.datatransform.DataTransform
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {
    val listBuffer = new ListBuffer[(Long, Int)]()
    val sparkSession = SparkSession.builder
      .appName("SparkDataSetExample")
      .getOrCreate()

    setConf(sparkSession)

    //Setting source and target data locations
    val hotelsDataLocation = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels"
    val weatherDataLocation = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather"
    val storeDataLocation = "abfss://data@stslizhwesteurope.dfs.core.windows.net/enriched"

    //Read source data
    val hotelsDf = sparkSession.read.option("header", "true").csv(hotelsDataLocation)
    val weatherDf = sparkSession.read.parquet(weatherDataLocation)

    //Transform original data
    val hotelsWithHashDf = DataTransform.getHotelsWithHashDf(hotelsDf)
    val weatherWithHashDf = DataTransform.getWeatherWithHashDf(weatherDf)

    //Join hotels and weather by hash and write data to storage
    weatherWithHashDf
      .join(hotelsWithHashDf, weatherWithHashDf("hash") === hotelsWithHashDf("Hash"), "left")
      .drop(hotelsWithHashDf("Hash"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(storeDataLocation)

    sparkSession.stop()
    System.exit(0)
  }

  /**
   * Setting configuration
   *
   * @param sparkSession the spark session
   */
  def setConf(sparkSession: SparkSession): Unit = {
    sparkSession.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
    sparkSession.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    sparkSession.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "XXX")
    sparkSession.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "XXX")
    sparkSession.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "XXX")

    val storageAccount: String = "stslizhwesteurope"
    val appId: String = "XXX"
    val clientSecret: String = "XXX"

    sparkSession.conf.set("fs.azure.account.auth.type." + storageAccount + ".dfs.core.windows.net", "OAuth")
    sparkSession.conf.set("fs.azure.account.oauth.provider.type." + storageAccount + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    sparkSession.conf.set("fs.azure.account.oauth2.client.id." + storageAccount + ".dfs.core.windows.net", appId)
    sparkSession.conf.set("fs.azure.account.oauth2.client.secret." + storageAccount + ".dfs.core.windows.net", clientSecret)
    sparkSession.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccount + ".dfs.core.windows.net", "XXX")

  }
}
