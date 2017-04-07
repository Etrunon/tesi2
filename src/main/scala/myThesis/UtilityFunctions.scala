package myThesis

import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 04/03/17.
  */
object UtilityFunctions {

  def readGraph(sc: SparkContext, edgeFile: String): Graph[(Long, Long), Long] = {
    val edgeDelimiter = ","

    // Parse the input file. If there are 2 int on each line edge weight is 1 by default. If there are 3 input on each line
    // edge weight is the last one.
    // If there are 0, 1 or more than 3 element per row throw exception
    val edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(edgeDelimiter).map(_.trim())
      tokens.length match {
        case 2 =>
          new Edge(tokens(0).toLong, tokens(1).toLong, 1L)
        case 3 =>
          new Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong)
        case _ =>
          throw new IllegalArgumentException("invalid input line: " + row)
      }
    })
    Graph.fromEdges(edgeRDD, (-1L, -1L))
  }

  def saveResultBulk(result: ListBuffer[String]): Unit = {
    val pw = new PrintWriter(new FileOutputStream(System.getProperty("output_path") + "/Result.txt", true))
    result.foreach(line => pw.append(line + "\n"))
    pw.close()
  }

  def saveSingleLine(line: String): Unit = {
    val pw = new PrintWriter(new FileOutputStream(System.getProperty("output_path") + "/Result.txt", true))
    pw.append(line + "\n")
    pw.close()
  }

  def modularity(graph: Graph[myVertex, Long]): Double = {
    if (graph.triplets.count() > 0) {
      val totEdges = graph.edges.count() / 2.0

      // If srcCom and dstCom are equal save 1 and then make the sum of them all
      val primaParte = graph.triplets.map(trip => if (trip.srcAttr.comId == trip.dstAttr.comId) 1.0 else 0.0).reduce((a, b) => a + b) / 2.0

      //Lista di funzioni da riga-vertice a valore y da togliere alla mod.
      val functionList = graph.vertices.map(x =>
        List((pay: (VertexId, myVertex)) => if (pay._2.comId == x._2.comId && x._1 != pay._1) {
          -pay._2.degree.toFloat * x._2.degree.toFloat / (2.0 * totEdges)
        } else 0.0)
      ).reduce((a, b) => a ::: b)
      //Mappando ai vertici una funzione che mappa a tutte le funzioni nella mia lista ogni vertice e sommando tutto
      // a ritroso si ha il risultato
      val secondaParte = graph.vertices.map(ver => functionList.map(f => f(ver)).sum).reduce((a, b) => a + b)

      (1.0 / (4.0 * totEdges)) * (primaParte + secondaParte)
    } else
      0.0
  }

  def loadAndPrepareGraph(file: String, sc: SparkContext): Graph[myVertex, VertexId] = {

    // create the graph from the file and add util data: (degree, commId)
    val graphLoaded: Graph[(Long, Long), Long] = readGraph(sc, file)
    val tmpGraph = pruneLeaves(graphLoaded, sc)
    val degrees = tmpGraph.degrees
    // Generate a graph with the correct formatting
    tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(degOpt.getOrElse(0).toLong / 2, id, id) }
  }

  def getVertexFromComm(commRDD: RDD[Community], sc: SparkContext): RDD[(Long, myVertex)] = {
    val chee = commRDD.map(c => c.members.toList).reduce((a, b) => a ::: b)
    sc.parallelize(chee.map(v => (v.verId, v)))
  }


  /**
    * Functions to prune leaves recursively from the graph. It takes as input a graph "undirected" (each edge has its symmetric) and prune the present leaves
    *
    * @param graph to be pruned
    * @param sc    spark context, to broadcast a filterlist
    * @return
    */
  def pruneLeaves(graph: Graph[(Long, Long), Long], sc: SparkContext): Graph[(Long, Long), Long] = {
    var removed = false
    var graph2 = graph

    do {
      removed = false
      val leaves = graph2.degrees.filter(v => v._2 <= 2)

      if (leaves.count() > 0) {
        val leavesBC = sc.broadcast(leaves.map(v => Set(v._1)).reduce((a, b) => a ++ b))
        removed = true
        val newVertices = graph2.vertices.filter(v => !leavesBC.value.contains(v._1))
        val newEdges = graph2.edges.filter(e => !leavesBC.value.contains(e.srcId) && !leavesBC.value.contains(e.dstId))

        graph2 = Graph(newVertices, newEdges)
      }
    } while (removed)

    //Take away vertices without any edge (they're not returned with .degree)
    if (graph2.vertices.count() > 0) {
      val goodVer = if (graph2.triplets.count() > 0) graph2.triplets.map(t => Set(t.srcId, t.dstId)).reduce((a, b) => a ++ b) else Set[VertexId]()
      val bcGoodVer = sc.broadcast(goodVer)
      val newVer = graph2.vertices.filter(v => bcGoodVer.value.contains(v._1))

      graph2 = Graph(newVer, graph2.edges)
    }

    graph2
  }

}
