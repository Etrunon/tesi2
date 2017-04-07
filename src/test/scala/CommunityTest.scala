/**
  * Created by etrunon on 13/02/17.
  */

import myThesis.MyMain.strategicCommunityFinder
import myThesis.UtilityFunctions.{loadAndPrepareGraph, modularity, pruneLeaves, readGraph}
import myThesis.{Community, myVertex}
import org.apache.log4j.{Level, Logger}
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

class CommunityTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private var sc: SparkContext = null
  val epsilon = math.pow(10, -6)
  var edgeFile = "RunData/Input/processed_unit.csv"
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override protected def beforeAll(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
    sc = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  class Fixtures {
    val graphLoaded: Graph[(Long, Long), Long] = {
      readGraph(sc, edgeFile)
    }
    val emptyVertex = new myVertex(0, 0, 0)
    val emptyCommunity = new Community(0, 0.0, new ListBuffer[myVertex]())
    val triangle_test: Triangle_test = new Triangle_test()
  }

  def fixture = new Fixtures

  "Spark" should "load graph from file VERTICES" in {
    edgeFile = "RunData/Input/processed_unit.csv"
    val graph = fixture.graphLoaded
    graph.vertices.count() should be(3)
    graph.edges.count() should be(6)
  }

  it should "load graph with the preparation step" in {
    edgeFile = "RunData/Input/processed_unit.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)
    graph.vertices.count() should be(3)
    graph.edges.count() should be(6)
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

  it should "switch community id" in {
    val v0 = fixture.emptyVertex
    val c0 = new Community(1, 0.0, ListBuffer[myVertex]())
    val c1 = new Community(2, 0.0, ListBuffer[myVertex]())

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

  "Mini Star graph" should "have total modularity of -1/16" in {
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

  "Star graph" should "have a total modularity of -90/800" in {
    val c = fixture.emptyCommunity

    c.addToComm(new myVertex(10, 0, -1L), 0, 10)

    for (i <- 0 to 9) {
      c.addToComm(new myVertex(1, 0, i), 1, 10)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-90.0 / 800.0}")
    math.abs(c.modularity - (-90.0 / 800.0)) should be < epsilon

  }

  it should "have -90/800 of modularity in its extremis" in {
    val c = fixture.emptyCommunity

    for (i <- 0 to 9) {
      c.addToComm(new myVertex(1, 0, i), 0, 10)
      println(c.modularity)
    }

    println(c.members.size)
    println(s"C mod ${c.modularity} should be ${-90.0 / 800.0}")
    math.abs(c.modularity - (-90.0 / 800.0)) should be < epsilon
  }

  it should "be able to remove each vertex down to 0 correctly" in {
    val c = fixture.emptyCommunity
    val modList = ListBuffer[Double]()

    c.addToComm(new myVertex(10, 0, -1L), 0, 10)
    modList += c.modularity
    for (i <- 0 to 9) {
      c.addToComm(new myVertex(1, 0, i), 1, 10)
      modList += c.modularity
    }

    val revList = modList.reverse

    for (i <- 0 to 9) {
      println(s"Mod ${c.modularity} - ${revList(i)}")
      math.abs(c.modularity - revList(i)) should be < epsilon
      c.removeFromComm(new myVertex(1, 0, i), 1, 10)
    }
  }

  "A square graph" should "have -1/8 total modularity" in {
    val c0 = new Community(0, 0.0, ListBuffer[myVertex]())

    c0.addToComm(new myVertex(2, 0, 0), 0, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(2, 0, 1), 1, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(2, 0, 2), 1, 4)
    println(c0.modularity)
    c0.addToComm(new myVertex(2, 0, 3), 2, 4)
    println(c0.modularity)

    c0.modularity should be(-1.0 / 8.0)
  }

  "Potential Vertex Gain" should "be computed (on a square graph)" in {
    val c0 = new Community(0, 0.0, ListBuffer[myVertex]())
    val c1 = new Community(1, 0.0, ListBuffer[myVertex]())

    val v0 = new myVertex(2, 0, 0)
    c0.addToComm(v0, 0, 4)
    c0.addToComm(new myVertex(2, 0, 1), 1, 4)

    val v1 = new myVertex(2, 0, 2)
    c1.addToComm(v1, 0, 4)
    c1.addToComm(new myVertex(2, 0, 3), 1, 4)

    val mod = c0.modularity
    val gain = c0.potentialVertexGain(v1, 1, 4)
    gain should be(c1.potentialVertexGain(v0, 1, 4))

    c0.addToComm(v1, 1, 4)

    c0.modularity should be(gain + mod)
  }

  it should "be stored into each node (in a square graph)" in {
    // This test takes advantage of the fact that modularity in a square is 0, 0, 0.625
    val c0 = fixture.emptyCommunity
    val v0 = new myVertex(2, 0, 0)
    c0.addToComm(v0, 0, 4)

    v0.potentialLoss should be(0.0)

    val v1 = new myVertex(2, 0, 1)
    c0.addToComm(v1, 1, 4)

    v1.potentialLoss should be(-c0.modularity)

    val v2 = new myVertex(2, 0, 2)
    c0.addToComm(v2, 1, 4)

    v2.potentialLoss should be(-c0.modularity)
  }

  "Prune Leaves" should "prune leaves" in {
    edgeFile = "RunData/Input/processed_unitLeaves.csv"

    val graph = fixture.graphLoaded
    val prunedGraph = pruneLeaves(graph, sc)

    prunedGraph.vertices.count should be(3)
    prunedGraph.edges.count should be(6)

    // Get only vertices ids and they should be 1 2 3
    prunedGraph.vertices.collect().map(v => v._1).toSet should be(Set(1, 2, 3))
  }

  it should "delete a single chain graph" in {
    edgeFile = "RunData/Input/processed_unitChain.csv"

    val graph = fixture.graphLoaded
    val prunedGraph = pruneLeaves(graph, sc)

    prunedGraph.vertices.count should be(0)
    prunedGraph.edges.count should be(0)
  }

  it should "delete all graph in Ministar" in {
    edgeFile = "RunData/Input/processed_unitMinistar.csv"

    val graph = fixture.graphLoaded
    val prunedGraph = pruneLeaves(graph, sc)

    prunedGraph.vertices.count should be(0)
    prunedGraph.edges.count should be(0)

  }

  it should "delete all graph in Star" in {
    edgeFile = "RunData/Input/processed_unitStar.csv"

    val graph = fixture.graphLoaded
    val prunedGraph = pruneLeaves(graph, sc)

    prunedGraph.vertices.count should be(0)
    prunedGraph.edges.count should be(0)

  }

  "Single modularity fun" should "be correct in Triangle graph (-1/12)" in {
    edgeFile = "RunData/Input/processed_unit.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.degree, 1L, v._2.verId))
    }), graph.edges, new myVertex(0, -1, -1))

    math.abs(modularity(newGraph) - (-1.0 / 12.0)) should be < epsilon
  }

  it should "be correct in MiniStar whole graph (0)" in {
    edgeFile = "RunData/Input/processed_unitMinistar.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)
    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.degree, 1L, v._2.verId))
    }), graph.edges, new myVertex(0, -1, -1))

    val result = modularity(newGraph)
    println(s"Test result: $result, should be ${-1.0 / 16.0}")

    math.abs(result - 0) should be < epsilon
  }

  it should "be correct in Star whole graph (0)" in {
    edgeFile = "RunData/Input/processed_unitStar.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.degree, 1L, v._2.verId))
    }), graph.edges, new myVertex(0, -1, -1))

    math.abs(modularity(newGraph) - 0) should be < epsilon
  }

  it should "be correct in DoubleTriangle whole graph (-3/25)" in {
    edgeFile = "RunData/Input/processed_unitDoubleTriangle.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.degree, 1L, v._2.verId))
    }), graph.edges, new myVertex(0, -1, -1))

    val result = modularity(newGraph)
    println(s"Test result: $result, should be ${-(3.0 / 25.0)}")

    math.abs(result - (-3.0 / 25.0)) should be < epsilon
  }

  it should "be correct in DoubleTriangle only one triangle (-3/50)" in {
    edgeFile = "RunData/Input/processed_unitDoubleTriangle.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)
    val comm = List(1, 2, 3)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.degree, if (comm.contains(v._2.verId)) 1L else v._2.verId, v._2.verId))
    }), graph.edges, new myVertex(0, -1, -1))

    val result = modularity(newGraph)

    println(s"\n\nComm Test: NewGraph vertices")
    newGraph.vertices.collect().foreach(println)

    println(s"Test result: $result, should be ${-3.0 / 50.0}")

    math.abs(result - (-3.0 / 50.0)) should be < epsilon
  }

  "Delta optimization" should "compute the correct modularity value" in {
    edgeFile = "RunData/Input/processed_mini1.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)


    val commRDD = strategicCommunityFinder(graph, 30, sc)

    println(s"f" * 200)
    println(s"\n\nComm Test: Total Edges: ${graph.edges.count() / 2}")
    println(s"\n\nComm Test: Communities found")
    commRDD.collect().foreach(x => println(x))

    commRDD.collect().foreach(c => {
      val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
        (v._2.verId, new myVertex(v._2.degree, if (c.members.contains(v._2)) -1000L else v._2.verId, v._2.verId))
      }), graph.edges, new myVertex(0, -1, -1))

      println(s"\n\nComm Test: NewGraph vertices")
      newGraph.vertices.collect().foreach(println)

      println(s"Community ${c.comId}: ${c.shortMembers()} ==> mod: ${c.modularity} - single ${modularity(newGraph)})")

      math.abs(c.modularity - modularity(newGraph)) should be < epsilon
    })
  }

  "Scheduler" should "schedule a single operazion" in {

  }

}