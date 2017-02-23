package myThesis

import java.io.File
import java.util.Date

import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


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
    val edgedelimiter = ","

    System.setProperty("output_path", outputPath)

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

    // create the graph from the file and add util data: (degree, commId)
    val tmpGraph: Graph[(Long, Long), Long] = Graph.fromEdges(edgeRDD, (-1L, -1L))

    //ToDo remove testing community handler
    val tmpComm = List(1, 2, 3, 4)
    val degrees = tmpGraph.degrees.cache()
    // (Degree, CommId)
    val graph: Graph[(Long, Long), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong / 2, if (tmpComm.contains(id)) 1L else id) }
    val totEdges = graph.edges.count() / 2

    graph.vertices.foreach(println)

    val primaParte = graph.mapTriplets(trip => if (trip.srcAttr._2 == trip.dstAttr._2) 1L else 0L)

    /*
        val funRDD = graph.vertices.map(x =>
          List((pay: (VertexId, (Long, Long))) => if (pay._2._2 == x._2._2 && x._1 != pay._1) {
            println(s"Esterno Nodo   ${pay._1}, (deg: ${pay._2._1}, comm: ${pay._2._2})")
            println(s"Rinchiuso Nodo ${x._1}, (deg: ${x._2._1}, comm: ${x._2._2})")
            pay._2._1.toFloat * x._2._1.toFloat
          } /*/ (2 * totEdges)*/)
        ).reduce((a, b) => a ::: b)
    */

    //Lista di funzioni da riga-vertice a valore y da togliere alla mod.
    val funRDD = graph.vertices.map(x =>
      List((pay: (VertexId, (Long, Long))) => if (pay._2._2 == x._2._2 && x._1 != pay._1) {
        println(s"Esterno Nodo   ${pay._1}, (deg: ${pay._2._1}, comm: ${pay._2._2})")
        println(s"Rinchiuso Nodo ${x._1}, (deg: ${x._2._1}, comm: ${x._2._2})")
        println(pay._2._1.toFloat * x._2._1.toFloat)
        -pay._2._1.toFloat * x._2._1.toFloat / (2 * totEdges)
      } else 0.0)
    ).reduce((a, b) => a ::: b)

    //Mappando ai vertici una funzione che mappa a tutte le funzioni nella mia lista ogni vertice e sommando tutto
    // a ritroso si ha il risultato
    graph.vertices.map(ver => funRDD.map(f => f(ver)).reduce((a, b) => a + b)).reduce((a, b) => a + b)
    println("/" * 90)

    // Test senza il grafo rdd di mezzo. Fa sostanzialmente la stessa cosa (anche con gli stessi valori in effetti
    val nodes = List(
      (4L, (4L, 1L)), (16L, (1L, 16L)), (22L, (1L, 22L)), (28L, (5L, 28L)), (30L, (5L, 30L)), (14L, (2L, 14L)), (24L, (1L, 24L)),
      (6L, (6L, 6L)), (18L, (1L, 18L)), (20L, (2L, 20L)), (8L, (4L, 8L)), (26L, (6L, 26L)), (10L, (3L, 10L)), (2L, (5L, 1L)),
      (19L, (1L, 19L)), (21L, (2L, 21L)), (15L, (1L, 15L)), (25L, (2L, 25L)), (29L, (6L, 29L)), (11L, (4L, 11L)), (27L, (5L, 27L)),
      (23L, (1L, 23L)), (1L, (4L, 1L)), (17L, (1L, 17L)), (3L, (4L, 1L)), (7L, (2L, 7L)), (9L, (4L, 9L)), (31L, (5L, 31L)), (5L, (4L, 5L))
    )

    val test = nodes.map(ver => funRDD.map(f => f(ver)).reduce((a, b) => a + b)).reduce((a, b) => a + b)
    println(s"Test, somm a finale: $test, andra' divisa per due? ${test / 2}")

    //    graph.vertices.map(vert => {
    //      println(vert)
    //      for (elem <- funRDD) {
    //      elem
    //      println(elem (7L, (1, 7)))
    //      }
    //      println(vert)
    //    }).count()

    //        funRDD.collect().foreach(x => println(x(1,1)))


    //        val cherobbae = graph.vertices.map(x =>
    //          graph.vertices.filter(ver => ver._2._2 == x._2._2 && ver._1 != x._1).map(y =>
    //            x._2._1.toFloat * y._2._1.toFloat / (2 * totEdges)
    //          ).reduce((a, b) => a + b)
    //        ).reduce((a, b) => a + b)
    //        println(s"Cherobbae'?! $cherobbae")

    //    primaParte.edges.foreach(println)
    //    println(s"Prima parte: ${primaParte.edges.}")
    //    println(s"Seconda parte: ${secParte.value}")
    //    val colMat: IndexedRowMatrix = new IndexedRowMatrix(graph.vertices.map(ver =>
    //      // Create an indexed row for each vertice with data:
    //      // community as index
    //      // Id and degree as values
    //      IndexedRow(ver._2._2, new DenseVector(Array(ver._1.toFloat, ver._2._1.toFloat)))
    //    ))
    //
    //    println(s"Rows of matrix: ${colMat.numRows()} Columns ${colMat.numCols()}")
    //
    //    colMat.rows.collect().foreach(println)

  }
}