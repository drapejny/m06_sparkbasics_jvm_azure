
import by.slizh.sparkbasics.utils.Utils
import org.scalatest.funsuite.AnyFunSuite

class MainTestSuite extends AnyFunSuite {

  test("Correct address test") {
    val address = "US Saint Mary 200 Second St"
    val coords = Utils.getCoordinates(address)
    assert(coords._1 != "0" && coords._2 != "0")
  }

  test("Wrong address test") {
    val address = "skdflasdlkfjsdfjlksdfjlk"
    val coords = Utils.getCoordinates(address)
    assert(coords._1 == "0" && coords._2 == "0")
  }
}
