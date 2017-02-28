package myThesis

import java.io.{File, _}
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 13/02/17.
  */
object MyMain {
  def main(args: Array[String]): Unit = {
    //List of communities to test code
    val testBundle = List[List[Long]](
      List(1),
      List(1, 2),
      List(1, 2, 3),
      List(1, 2, 3, 4),
      List(1, 2, 3, 4, 5),
      List(1, 2, 3, 4, 5, 6),
      List(1, 2, 3, 4, 5, 6, 14),
      List(1, 2, 3, 4, 5, 6, 14, 15),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28),
      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28, 23, 8)
    )

    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Sets source folder
    val edgeFile = "RunData/Input/processed_mini1.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm:ss").format(new Date())
    val outputDir = new File(outputPath)
    outputDir.mkdirs()
    System.setProperty("output_path", outputPath)

    // create the graph from the file and add util data: (degree, commId)
    val graphLoaded: Graph[(Long, Long), Long] = readGraph(sc, edgeFile)

    val res1 = testBundleTestModularity(testBundle, graphLoaded)
    val res2 = testBundleTestMigration(testBundle, graphLoaded)

    res1.foreach(println)
    res2.foreach(println)

    //    readInt()
  }


  def testBundleTestMigration(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
    //Timed execution
    val initDate = System.currentTimeMillis()
    val degrees = graphLoaded.degrees.cache()
    val result = ListBuffer[String]()
    result += "Migration"

    // Generate a graph with the correct formatting
    var graph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(degOpt.getOrElse(0).toLong / 2, id, id) }
    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    // Initialization of delta system. (The graph initially has one vertex for each community, so the first delta should be 0 )
    // Moreover modularity of a single vertex is zero by default
    var oldCom = List[Long](1L)
    // CommId of the vertex will be 1, for testing purpose
    val newCom = 1L

    // Foreach community inside the bundle
    for (com <- testBundle) {

      // Take only those Id which represent the delta since last computation
      com.filterNot(oldCom.contains(_)).foreach(id => {

        // Take the reference of that vertex. In spite of ".first()" there should always be only one value
        val switchingVertex: myVertex = graph.vertices.filter(v => v._1 == id).values.first()

        // Count edges inside the old community to be subtracted and count edges inside the new community to be added
        //(OldCommunity, NewCommunity)
        val oldComPointer = commRDD.filter(c => c.comId == switchingVertex.comId).first()
        val newComPointer = commRDD.filter(c => c.comId == newCom).first()
        val edgesChange = graph.triplets.filter(tri => tri.srcId == switchingVertex.verId).map(tri => tri.dstAttr).map(dstId => {
          if (oldComPointer.members.contains(dstId)) (1, 0)
          else if (newComPointer.members.contains(dstId)) (0, 1)
          else (0, 0)
        }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

        // Foreach community inside the list, update modularity values in the communityRDD
        val commRDD1 = commRDD.map(c => {
          // If the community is the old one
          if (c.comId == switchingVertex.comId)
            c.removeFromComm(switchingVertex, edgesChange._1, totEdges)

          //Else if the community is the new one
          else if (c.comId == newCom)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          c
        })
        commRDD = commRDD1
        //        commRDD.count()
      })

      result += s"Modularity of: $com:\t ${commRDD.map(c => c.modularity).reduce((c, v) => c + v)}"

      //Update Graph to mark all new comm members
      val newEdges = graph.edges
      val newVertices = graph.vertices.map(ver => {
        if (com.contains(ver._2.verId)) ver._2.comId = newCom
        ver
      })

      graph = Graph(newVertices, newEdges)

      oldCom = com
    }
    val endDate = System.currentTimeMillis()
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }

  def testBundleTestModularity(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
    val initDate = System.currentTimeMillis()

    val degrees = graphLoaded.degrees.cache()
    val result = ListBuffer[String]()
    result += "Whole Computation"

    for (tmpComm <- testBundle) {
      // (Degree, CommId)
      val tmpGraph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) =>
        new myVertex(degOpt.getOrElse(0).toLong / 2, if (tmpComm.contains(id)) 1L else id, id)
      }
      //      println(s"Modularity of $tmpComm:\t ${modularity(graph)}")
      val s = s"Modularity of $tmpComm:\t ${modularity(tmpGraph)}"
      result += s
    }
    val endDate = System.currentTimeMillis()
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }

  def modularity(graph: Graph[myVertex, Long]): Double = {
    val totEdges = graph.edges.count() / 2

    val primaParte = graph.mapTriplets(trip => if (trip.srcAttr.comId == trip.dstAttr.comId) 1L else 0L).edges.reduce((a, b) => new Edge[Long](0, 0, a.attr + b.attr)).attr / 2

    //Lista di funzioni da riga-vertice a valore y da togliere alla mod.
    val functionList = graph.vertices.map(x =>
      List((pay: (VertexId, myVertex)) => if (pay._2.comId == x._2.comId && x._1 != pay._1) {
        -pay._2.degree.toFloat * x._2.degree.toFloat / (2 * totEdges)
      } else 0.0)
    ).reduce((a, b) => a ::: b)
    //Mappando ai vertici una funzione che mappa a tutte le funzioni nella mia lista ogni vertice e sommando tutto
    // a ritroso si ha il risultato
    val secondaParte = graph.vertices.map(ver => functionList.map(f => f(ver)).sum).reduce((a, b) => a + b)

    (1.0 / (4.0 * totEdges)) * (primaParte + secondaParte)
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

  def saveResultBulk(result: ListBuffer[String]): Unit = {
    val pw = new PrintWriter(new File(System.getProperty("output_path") + "/Result.txt"))
    result.foreach(line => pw.write(line + "\n"))
    pw.close()
  }

  /*
    def changeCommunity(ver: (VertexId, (Long, Long)), comId: Long, comRDD: RDD[Community]): Unit = {
      println(s"Changing $ver to comm $comId")

      comRDD.map(com => {
        if (com.comId == comId) {
          println(s"Adding ver to comm ${com.comId}")
          com.members += ver._1
          //ToDo update modularity
        } else if (com.comId == ver._2._2) {
          println(s"Removing ver from com ${com.comId}")
          com.members -= ver._1
          // ToDo update modularity
        }
      })
      //ToDo non viene lanciato l'update all'rdd
    }
  */


}