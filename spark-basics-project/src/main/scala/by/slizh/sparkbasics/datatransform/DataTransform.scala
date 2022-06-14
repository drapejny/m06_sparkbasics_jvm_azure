package by.slizh.sparkbasics.datatransform

import by.slizh.sparkbasics.utils.Utils
import by.slizh.sparkbasics.utils.Utils.getGeoHash
import org.apache.spark.sql.DataFrame

object DataTransform {

  /**
   * Correcting hotels data and generating geohash by lat and lng
   * @param hotelsDf the original hotels dataframe
   * @return hotels dataframe with correct lat and lng and generated hash
   */
  def getHotelsWithHashDf(hotelsDf: DataFrame): DataFrame = {
    import hotelsDf.sparkSession.implicits._
    val hotelsWithHashDf = hotelsDf.map(row => {
      var lat = row.getString(5)
      var lng = row.getString(6)
      if (lat == null || lng == null || lng == "NA" || lat == "NA") {
        val address = row.getString(2) + " " + row.getString(3) + " " + row.getString(4)
        val coords = Utils.getCoordinates(address)
        lat = coords._1
        lng = coords._2
      }
      val hash = getGeoHash(lat.toDouble, lng.toDouble)
      (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), lat, lng, hash)
    })
      .filter(x => x._1 != "0")
      .toDF("Id", "Name", "Country", "City", "Address", "Latitude", "Longitude", "Hash")

    hotelsWithHashDf
  }

  /**
   * Generating geohash by lat and lng for original dataframe
   * @param weatherDf the original weather dataframe
   * @return weather dataframe with generated hash
   */
  def getWeatherWithHashDf(weatherDf: DataFrame): DataFrame = {
    import weatherDf.sparkSession.implicits._
    val weatherWithHashDf = weatherDf.map(row => {
      val lng = row.getDouble(0)
      val lat = row.getDouble(1)
      val hash = getGeoHash(lat, lng)
      (lng, lat, row.getDouble(2), row.getDouble(3), row.getString(4), row.getInt(5), row.getInt(6), row.getInt(7), hash)
    })
      .toDF("lng", "lat", "avg_tmpr_f", "avg_tmpr_c", "wthr_date", "wthr_year", "wthr_month", "wthr_day", "hash")
    weatherWithHashDf
  }
}
