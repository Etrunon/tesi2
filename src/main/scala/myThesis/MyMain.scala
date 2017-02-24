package myThesis

import java.io.{File, _}
import java.util.Date

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 13/02/17.
  */
object MyMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Sets source folder
    val edgeFile = "RunData/Input/processed_mini1.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm:ss").format(new Date())
    val outputDir = new File(outputPath)
    outputDir.mkdirs()
    System.setProperty("output_path", outputPath)

    // create the graph from the file and add util data: (degree, commId)
    val tmpGraph: Graph[(Long, Long), Long] = readGraph(sc, edgeFile)
    val degrees = tmpGraph.degrees.cache()

    //ToDo remove testing community handler
    val testBundle = List[List[Long]](
      List(1),
      List(1, 2),
      List(1, 2, 3),
      List(1, 2, 3, 4),
      List(1, 2, 3, 4, 5),
      List(1, 2, 3, 4, 5, 6),
      List(1, 2, 3, 4, 5, 6, 7),
      List(1, 2, 3, 4, 5, 6, 14),
      List(1, 2, 3, 4, 5, 6, 14, 15),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 23, 8)
    )

    val result = ListBuffer[String]()
    for (tmpComm <- testBundle) {
      // (Degree, CommId)
      val graph: Graph[(Long, Long), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong / 2, if (tmpComm.contains(id)) 1L else id) }
      //      println(s"Modularity of $tmpComm:\t ${modularity(graph)}")
      val s = s"Modularity of $tmpComm:\t ${modularity(graph)}"
      result += s
    }
    result.foreach(println)
    saveResultBulk(result)
  }

  def modularity(graph: Graph[(Long, Long), Long]): Double = {
    val totEdges = graph.edges.count() / 2

    //    graph.vertices.foreach(println)
    val primaParte = graph.mapTriplets(trip => if (trip.srcAttr._2 == trip.dstAttr._2) 1L else 0L).edges.reduce((a, b) => new Edge[Long](0, 0, a.attr + b.attr)).attr

    //Lista di funzioni da riga-vertice a valore y da togliere alla mod.
    val functionList = graph.vertices.map(x =>
      List((pay: (VertexId, (Long, Long))) => if (pay._2._2 == x._2._2 && x._1 != pay._1) {
        -pay._2._1.toFloat * x._2._1.toFloat / (2 * totEdges)
      } else 0.0)
    ).reduce((a, b) => a ::: b)

    //Mappando ai vertici una funzione che mappa a tutte le funzioni nella mia lista ogni vertice e sommando tutto
    // a ritroso si ha il risultato
    val secondaParte = graph.vertices.map(ver => functionList.map(f => f(ver)).sum).reduce((a, b) => a + b)

    val modularity = (1.0 / (4.0 * totEdges)) * (primaParte + secondaParte)
    modularity
  }

  def readGraph(sc: SparkContext, edgeFile: String): Graph[(Long, Long), Long] = {
    val edgedelimiter = ","

    // Parse the input file. If there are 2 int on each line edge weight is 1 by default. If there are 3 input on each line
    // edge weight is the last one.
    // If there are 0, 1 or more than 3 element per row throw exception
    val edgeRDD = sc.textFile(edgeFile).map(row => {
      val tokens = row.split(edgedelimiter).map(_.trim())
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

  def saveResultBulk(result: ListBuffer[String]) = {
    val pw = new PrintWriter(new File(System.getProperty("output_path") + "/Result.txt"))
    result.foreach(line => pw.write(line + "\n"))
    pw.close()
  }
}