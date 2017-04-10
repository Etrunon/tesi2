package tests

import myThesis.{Community, myVertex}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 10/04/17.
  */
class myStructuresTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  def fixture = new Fixtures

  class Fixtures {
    val emptyVertex = new myVertex(0, 0, 0)
    val emptyCommunity = new Community(0, new ListBuffer[myVertex](), 0.0)
    val triangle_test: Triangle_test = new Triangle_test()
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
    val ver2 = new myVertex(1, 0, 0)
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

  it should "switch community id" in {
    val v0 = fixture.emptyVertex
    val c0 = new Community(1, ListBuffer[myVertex](), 0.0)
    val c1 = new Community(2, ListBuffer[myVertex](), 0.0)

    val com0 = v0.comId
    c0.addToComm(v0, 0, 1)
    val com1 = v0.comId
    c1.addToComm(v0, 0, 1)
    val com2 = v0.comId

    com0 should not be com1
    com0 should not be com2
    com1 should not be com2

    // ToDo finire di vedere se il valore viene aggiornato nel momento in cui si cambia comunità
  }

  "Community" should "be created rightly when empty" in {
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
    fixture.emptyCommunity should not be new Community(1, new ListBuffer[myVertex](), 0.0)
  }

  it should "be different fron a community with same id but different members" in {
    fixture.emptyCommunity should not be new Community(0, ListBuffer(fixture.emptyVertex), 0.0)
  }

}
