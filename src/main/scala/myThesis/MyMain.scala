package myThesis

import java.io.File
import java.util.Date

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by etrunon on 13/02/17.
  */
object MyMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Sets source folder
    val edgeFile = "RunData/Input/mini1.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm").format(new Date())
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

    val degrees = tmpGraph.degrees
    val graph: Graph[(Long, Long, List[Long]), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong, id, List[Long](id)) }

    tmpGraph.triplets.collect().foreach(println)
  }
}
