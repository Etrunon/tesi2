package tests

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

class SparkTests extends FlatSpec with Matchers with BeforeAndAfterAll {
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
    def graphLoaded: Graph[(Long, Long), Long] = {
      readGraph(edgeFile, sc)
    }
    val emptyVertex = new myVertex(0, 0, 0)
    val emptyCommunity = new Community(0, new ListBuffer[myVertex](), 0.0)
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
      (v._2.verId, new myVertex(v._2.verId, 1L, v._2.degree))
    }), graph.edges, new myVertex(-1, -1, 0))

    math.abs(modularity(newGraph) - (-1.0 / 12.0)) should be < epsilon
  }

  it should "be correct in MiniStar whole graph (0)" in {
    edgeFile = "RunData/Input/processed_unitMinistar.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)
    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.verId, 1L, v._2.degree))
    }), graph.edges, new myVertex(-1, -1, 0))

    val result = modularity(newGraph)
    println(s"Test result: $result, should be ${-1.0 / 16.0}")

    math.abs(result - 0) should be < epsilon
  }

  it should "be correct in Star whole graph (0)" in {
    edgeFile = "RunData/Input/processed_unitStar.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.verId, 1L, v._2.degree))
    }), graph.edges, new myVertex(-1, -1, 0))

    math.abs(modularity(newGraph) - 0) should be < epsilon
  }

  it should "be correct in DoubleTriangle whole graph (-3/25)" in {
    edgeFile = "RunData/Input/processed_unitDoubleTriangle.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)

    val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
      (v._2.verId, new myVertex(v._2.verId, 1L, v._2.degree))
    }), graph.edges, new myVertex(-1, -1, 0))

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
      (v._2.verId, new myVertex(v._2.verId, if (comm.contains(v._2.verId)) 1L else v._2.verId, v._2.degree))
    }), graph.edges, new myVertex(-1, -1, 0))

    val result = modularity(newGraph)

    println(s"\n\nComm Test: NewGraph vertices")
    newGraph.vertices.collect().foreach(println)

    println(s"Test result: $result, should be ${-3.0 / 50.0}")

    math.abs(result - (-3.0 / 50.0)) should be < epsilon
  }

  "Delta optimization" should "compute the correct modularity value" in {
    edgeFile = "RunData/Input/processed_mini1.csv"
    val graph = loadAndPrepareGraph(edgeFile, sc)


    val commRDD = strategicCommunityFinder(graph, 30, sc)._1

    println(s"f" * 200)
    println(s"\n\nComm Test: Total Edges: ${graph.edges.count() / 2}")
    println(s"\n\nComm Test: Communities found")
    commRDD.collect().foreach(x => println(x))

    commRDD.collect().foreach(c => {
      val newGraph: Graph[myVertex, Long] = Graph.apply(graph.vertices.map(v => {
        (v._2.verId, new myVertex(v._2.verId, if (c.members.contains(v._2)) -1000L else v._2.verId, v._2.degree))
      }), graph.edges, new myVertex(-1, -1, 0))

      println(s"\n\nComm Test: NewGraph vertices")
      newGraph.vertices.collect().foreach(println)

      println(s"Community ${c.comId}: ${c.shortMembers()} ==> mod: ${c.modularity} - single ${modularity(newGraph)})")

      math.abs(c.modularity - modularity(newGraph)) should be < epsilon
    })
  }


}