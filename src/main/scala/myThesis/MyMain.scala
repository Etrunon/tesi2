package myThesis

import java.io.File
import java.util.Date

import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.DenseVector
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
    val tmpComm = sc.broadcast(List(1, 2, 3, 4))
    val degrees = tmpGraph.degrees.cache()
    // (Degree, CommId)
    val graph: Graph[(Long, Long), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong / 2, if (tmpComm.value.contains(id)) 1L else id) }


    val primaParte = graph.mapTriplets(trip => if (trip.srcAttr._2 == trip.dstAttr._2) 1)

    val colMat: IndexedRowMatrix = new IndexedRowMatrix(graph.vertices.map(ver =>
      // Create an indexed row for each vertice with data:
      // community as index
      // Id and degree as values
      IndexedRow(ver._2._2, new DenseVector(Array(ver._1.toFloat, ver._2._1.toFloat)))
    ))

    println(s"Rows of matrix: ${colMat.numRows()} Columns ${colMat.numCols()}")

    colMat.rows.collect().foreach(println)

  }
}