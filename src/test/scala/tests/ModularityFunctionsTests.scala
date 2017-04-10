package tests

import myThesis.{Community, myVertex}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 10/04/17.
  */
class ModularityFunctionsTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  val epsilon = math.pow(10, -6)
  var edgeFile = "RunData/Input/processed_unit.csv"

  def fixture = new Fixtures

  class Fixtures {
    val emptyVertex = new myVertex(0, 0, 0)
    val emptyCommunity = new Community(0, new ListBuffer[myVertex](), 0.0)
    val triangle_test: Triangle_test = new Triangle_test()
  }

  "Triangle graph" should "init inside the test environment" in {
    val tt = fixture.triangle_test

    tt.c0 should not be tt.c1
    tt.c0 should not be tt.c2
    tt.c1 should not be tt.c2
  }

  println("ciao")
  it should "entire modularity should be -1/12" in {
    val tt = fixture.triangle_test

    tt.c0.addToComm(tt.v1, 1, 3)
    tt.c0.addToComm(tt.v2, 2, 3)

    println(s"${tt.c0.modularity} - ${-1.0 / 12.0}} < $epsilon")
    println(s"${math.abs(tt.c0.modularity - (-1.0 / 12.0))} < $epsilon")
    math.abs(tt.c0.modularity - (-1.0 / 12.0)) should be < epsilon
  }

  it should "switch a vertex from c0 to c1 rightly" in {
    val tt = fixture.triangle_test

    tt.c1.addToComm(tt.v0, 1, 3)
    tt.c0.removeFromComm(tt.v0, 0, 3)

    val verSet = Set(tt.v0, tt.v1)
    tt.c1.members.toSet should be(verSet)
    tt.c0.members.length should be(0)
    println(s"${math.abs(tt.c1.modularity - 1.0 / 36.0)} < $epsilon")
    math.abs(tt.c1.modularity - (-1.0 / 36.0)) should be < epsilon
    tt.c0.modularity should be(0.0)
  }

  it should "undo the above operation rightly" in {
    val tt = fixture.triangle_test

    //Do part
    tt.c1.addToComm(tt.v0, 1, 3)
    tt.c0.removeFromComm(tt.v0, 0, 3)
    //Undo part
    tt.c0.addToComm(tt.v0, 0, 3)
    tt.c1.removeFromComm(tt.v0, 1, 3)

    tt.c0.members.length should be(1)
    tt.c0.members.contains(tt.v0) should be(true)

    tt.c1.members.length should be(1)
    tt.c1.members.contains(tt.v1) should be(true)

    tt.c0.modularity should be(0.0)
    tt.c1.modularity should be(0.0)
  }

  it should "add, remove and add again to a 3size community rightly" in {
    val tt = fixture.triangle_test

    //Community of 3
    tt.c0.addToComm(tt.v1, 1, 3)
    tt.c1.removeFromComm(tt.v1, 0, 3)
    tt.c0.addToComm(tt.v2, 2, 3)
    tt.c2.removeFromComm(tt.v2, 0, 3)

    //Remove one and add again
    var a = 0L
    do {
      tt.c0.removeFromComm(tt.v1, 2, 3)
      tt.c1.addToComm(tt.v1, 0, 3)

      tt.c0.addToComm(tt.v1, 2, 3)
      tt.c1.removeFromComm(tt.v1, 2, 3)
      a += 1
    } while (a < 1000L)

    println(s"${tt.c0.modularity} - ${-1.0 / 12.0}")
    println(s"${math.abs(tt.c0.modularity - (-1.0 / 12.0))} < $epsilon")
    math.abs(tt.c0.modularity - (-1.0 / 12.0)) should be < epsilon

    tt.c1.modularity should be(0.0)
    tt.c2.modularity should be(0.0)
  }

  "Mini Star graph" should "have total modularity of -1/16" in {
    val c = fixture.emptyCommunity

    c.addToComm(new myVertex(-1L, 0, 2), 0, 2)

    for (i <- 0 to 1) {
      c.addToComm(new myVertex(i, 0, 1), 1, 2)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-1.0 / 16.0}")
    math.abs(c.modularity - (-1.0 / 16.0)) should be < epsilon
  }

  "Star graph" should "have a total modularity of -90/800" in {
    val c = fixture.emptyCommunity

    c.addToComm(new myVertex(-1L, 0, 10), 0, 10)

    for (i <- 0 to 9) {
      c.addToComm(new myVertex(i, 0, 1), 1, 10)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-90.0 / 800.0}")
    math.abs(c.modularity - (-90.0 / 800.0)) should be < epsilon

  }

  it should "have -90/800 of modularity in its extremis" in {
    val c = fixture.emptyCommunity

    for (i <- 0 to 9) {
      c.addToComm(new myVertex(i, 0, 1), 0, 10)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-90.0 / 800.0}")
    math.abs(c.modularity - (-90.0 / 800.0)) should be < epsilon
  }

  it should "be able to remove each vertex down to 0 correctly" in {
    val c = fixture.emptyCommunity
    val modList = ListBuffer[Double]()

    c.addToComm(new myVertex(-1L, 0, 10), 0, 10)
    modList += c.modularity
    for (i <- 0 to 9) {
      c.addToComm(new myVertex(i, 0, 1), 1, 10)
      modList += c.modularity
    }

    val revList = modList.reverse

    for (i <- 0 to 9) {
      println(s"Mod ${c.modularity} - ${revList(i)}")
      math.abs(c.modularity - revList(i)) should be < epsilon
      c.removeFromComm(new myVertex(i, 0, 1), 1, 10)
    }
  }

  "A square graph" should "have -1/8 total modularity" in {
    val c0 = new Community(0, ListBuffer[myVertex](), 0.0)

    c0.addToComm(new myVertex(0, 0, 2), 0, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(1, 0, 2), 1, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(2, 0, 2), 1, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(3, 0, 2), 2, 4)
    println(c0.modularity)

    c0.modularity should be(-1.0 / 8.0)
  }

  "Potential Vertex Gain" should "be computed (on a square graph)" in {
    val c0 = new Community(0, ListBuffer[myVertex](), 0.0)
    val c1 = new Community(1, ListBuffer[myVertex](), 0.0)

    val v0 = new myVertex(0, 0, 2)
    c0.addToComm(v0, 0, 4)
    c0.addToComm(new myVertex(1, 0, 2), 1, 4)

    val v1 = new myVertex(2, 0, 2)
    c1.addToComm(v1, 0, 4)
    c1.addToComm(new myVertex(3, 0, 2), 1, 4)

    val mod = c0.modularity
    val gain = c0.potentialVertexGain(v1, 1, 4)
    gain should be(c1.potentialVertexGain(v0, 1, 4))

    c0.addToComm(v1, 1, 4)

    c0.modularity should be(gain + mod)
  }

  it should "be stored into each node (in a square graph)" in {
    // This test takes advantage of the fact that modularity in a square is 0, 0, 0.625
    val c0 = fixture.emptyCommunity
    val v0 = new myVertex(0, 0, 2)
    c0.addToComm(v0, 0, 4)

    v0.potentialLoss should be(0.0)

    val v1 = new myVertex(1, 0, 2)
    c0.addToComm(v1, 1, 4)

    v1.potentialLoss should be(-c0.modularity)

    val v2 = new myVertex(2, 0, 2)
    c0.addToComm(v2, 1, 4)

    v2.potentialLoss should be(-c0.modularity)
  }

}


class Triangle_test {
  val v0 = new myVertex(0, 0, 2)
  val v1 = new myVertex(1, 1, 2)
  val v2 = new myVertex(2, 2, 2)

  val c0 = new Community(0, ListBuffer[myVertex](v0), 0.0)
  val c1 = new Community(1, ListBuffer[myVertex](v1), 0.0)
  val c2 = new Community(2, ListBuffer[myVertex](v2), 0.0)
}


