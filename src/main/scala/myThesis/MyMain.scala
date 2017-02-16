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


    // Sets source folder
    val edgeFile = "RunData/Input/mini1.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm").format(new Date())
    val outputDir = new File(outputPath)
    outputDir.mkdirs()
    val edgedelimiter = ","

    // Sets spark Conf
    val sconf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("CommDec.Tesi2")
    val sc = new SparkContext(sconf)

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
    val graph: Graph[(Long, Long), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong, id) }

    /*
    val rawGraph: Graph[(),()] = Graph.textFile("twittergraph")
    val inDeg: RDD[(VertexId, Int)] =
      mapReduceTriplets[Int](et => Iterator((et.dst.id, 1)), _ + _)
    */

    //    println("Beginning things")
    val listNeighbours = (e: EdgeTriplet[(Long, Long), Long]) => {
      //      println(s"Src ${e.srcId}, Dst ${e.dstId}")
      val x = Iterator(e.srcAttr, e.dstAttr)
      //      x.foreach(println)
      x
    }
    //    println("Beginning other things")
    val compareNeighbours = (deg1: Long, deg2: Long) => {
      //      println(s"comparing $deg1, $deg2")
      Math.max(deg1, deg2)
    }
    //    println("Beginning final things")
    val maxDegNeig = graph.mapReduceTriplets(listNeighbours, compareNeighbours)
    maxDegNeig.collect().foreach(println)
  }

  class test(degree: Long, cid: Long) {
    override def toString: String = {
      "{deg:" + degree + ",cid:" + cid + "}"
    }

  }

}
