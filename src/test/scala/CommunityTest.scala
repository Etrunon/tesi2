/**
  * Created by etrunon on 13/02/17.
  */

import myThesis.{Community, UtilityFunctions, myVertex}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.mutable.ListBuffer

class Triangle_test {
  val v0 = new myVertex(2, 0, 0)
  val v1 = new myVertex(2, 1, 1)
  val v2 = new myVertex(2, 2, 2)

  val c0 = new Community(0, 0.0, ListBuffer[myVertex](v0))
  val c1 = new Community(1, 0.0, ListBuffer[myVertex](v1))
  val c2 = new Community(2, 0.0, ListBuffer[myVertex](v2))
}

class CommunityTest extends FlatSpec with Matchers {
  val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[3]")
  val sc = new SparkContext(conf)
  val epsilon = 0.000000001

  class Fixtures {
    val graphLoaded: Graph[(Long, Long), Long] = {
      val edgeFile = "RunData/Input/processed_unit.csv";
      UtilityFunctions.readGraph(sc, edgeFile)
    }
    val emptyVertex = new myVertex(0, 0, 0)
    val emptyCommunity = new Community(0, 0.0, new ListBuffer[myVertex]())
    val triangle_test: Triangle_test = new Triangle_test()

  }

  def fixture = new Fixtures

  "Spark" should "load graph from file VERTICES" in {
    fixture.graphLoaded.vertices.count() should be(3)
  }

  it should "load graph from file EDGES" in {
    fixture.graphLoaded.edges.count() should be(6)
  }

  "myVertex" should "be created rightly" in {
    val ver = fixture.emptyVertex
    ver.verId should be(0L)
    ver.comId should be(0L)
    ver.degree should be(0L)
    ver.potentialLoss should be(0.0)
  }

  it should "be equal to itself" in {
    fixture.emptyVertex should be(fixture.emptyVertex)
  }

  it should "be different from another" in {
    val ver2 = new myVertex(0, 0, 1)
    fixture.emptyVertex should not be ver2
  }

  it should "toString" in {
    val ver = fixture.emptyVertex
    ver.toString should be("Vertex 0 degree 0, member of comm 0")
  }

  it should "toString also in short form" in {
    val ver = fixture.emptyVertex
    ver.toStringShort should be("0, ")
  }

  "An empty Community" should "be created rightly" in {
    val c = fixture.emptyCommunity

    c.comId should be(0)
    c.modularity should be(0.0)
    c.members.length should be(0)
  }

  it should "be able to add one member" in {
    val c = fixture.emptyCommunity
    c.addToComm(fixture.emptyVertex, 0, 0)

    c.members.length should be(1)
    c.members.head should be(fixture.emptyVertex)
  }

  it should "be able to remove a member" in {
    val c = fixture.emptyCommunity
    c.addToComm(fixture.emptyVertex, 0, 0)

    c.removeFromComm(fixture.emptyVertex, 0, 0)

    c.members.length should be(0)
  }

  it should "be equal to itself" in {
    fixture.emptyCommunity should be(fixture.emptyCommunity)
  }

  it should "be different from a different community" in {
    fixture.emptyCommunity should not be new Community(1, 0.0, new ListBuffer[myVertex]())
  }

  it should "be different fron a community with same id but different members" in {
    fixture.emptyCommunity should not be new Community(0, 0.0, ListBuffer(fixture.emptyVertex))
  }

  "Triangle graph" should "init inside the test environment" in {
    val tt = fixture.triangle_test

    tt.c0 should not be tt.c1
    tt.c0 should not be tt.c2
    tt.c1 should not be tt.c2
  }

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

  "mini Star graph" should "have total modularity of -1/16" in {
    val c = fixture.emptyCommunity

    c.addToComm(new myVertex(2, 0, -1L), 0, 2)

    for (i <- 0 to 1) {
      c.addToComm(new myVertex(1, 0, i), 1, 2)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-1.0 / 16.0}")
    math.abs(c.modularity - (-1.0 / 16.0)) should be < epsilon
  }

  "Star graph" should "have a total modularity of -81/160" in {
    val c = fixture.emptyCommunity

    c.addToComm(new myVertex(10, 0, -1L), 0, 10)

    for (i <- 0 to 8) {
      c.addToComm(new myVertex(1, 0, i), 1, 10)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-81.0 / 160.0}")
    math.abs(c.modularity - (-81.0 / 160.0)) should be < epsilon

  }


}