package by.slizh.sparkbasics.utils

import ch.hsr.geohash.GeoHash
import com.opencagedata.geocoder.OpenCageClient
import org.apache.commons.cli._

import java.util.Properties
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Utils {

  /**
   * Provide coordinates by the full address
   *
   * @param address the full address in format: "country city address"
   * @return the tuple of two coordinates, if address is correct;
   *         otherwise,the tuple ("0", "0")
   */
  def getCoordinates(address: String): Tuple2[String, String] = {
    val client = new OpenCageClient("ecf061dba0f7481f9192dccca4524684")
    val responseFuture = client.forwardGeocode(address)
    val response = Await.result(responseFuture, 5 seconds)
    try {
      val geometry = response.results(0).geometry
      (geometry.get.lat.toString, geometry.get.lng.toString)
    } catch {
      case _: IndexOutOfBoundsException => ("0", "0")
    }
  }

  /**
   * Generate the 4 characters hash by latitude and longitude
   *
   * @param lat the latitude
   * @param lng the longitude
   * @return the 4 characters hash
   */
  def getGeoHash(lat: Double, lng: Double): String = {
    val NUM_OF_CHARACTERS = 4
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, NUM_OF_CHARACTERS)
  }
}
