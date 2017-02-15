/**
  * Created by etrunon on 13/02/17.
  */

import collection.mutable.Stack
import org.scalatest._

class ExampleSpec extends FlatSpec with Matchers {

  "The Main" should "return 100 if given 12" in {
    "12".toInt should be (12)
  }

  it should "return 100 if given 13" in {
    "12".toInt should be (13)
  }

  it should "return 100 if given 14" in {
    "12".toInt should be (14)
  }
  it should "return 100 if given 12 again" in {
    "12".toInt should be (12)
  }

  //  "The Main" should "return 100 when given 12" in {
//    val mym = new MyMain()
//    val z: Array[String] = Array("12")
//    print(mym.main(z))
//    mym.main(z) should be (100)
  //  }
}