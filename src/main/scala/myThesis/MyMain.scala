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
      List(1, 2, 3)
      //      List(1, 2, 3, 4)
      //      List(1, 2, 3, 4, 5)
      //      List(1, 2, 3, 4, 5, 6),
      //      List(1, 2, 3, 4, 5, 6, 14),
      //      List(1, 2, 3, 4, 5, 6, 14, 15),
      //      List(1, 2, 3, 4, 5, 6, 14, 15, 30),
      //      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28),
      //      List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28, 23, 8)
    )

    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Sets source folder
    val edgeFile = "RunData/Input/processed_micro.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm:ss").format(new Date())
    val outputDir = new File(outputPath)
    outputDir.mkdirs()
    System.setProperty("output_path", outputPath)

    // create the graph from the file and add util data: (degree, commId)
    val graphLoaded: Graph[(Long, Long), Long] = readGraph(sc, edgeFile)

    val res1 = testBundleTestModularity(testBundle, graphLoaded)
    val res2 = testBundleTestMigration(testBundle, graphLoaded)

    println(s"\n\nRisultati:\n")
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
    val graph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(degOpt.getOrElse(0).toLong / 2, id, id) }
    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    // Initialization of delta system. (The graph initially has one vertex for each community, so the first delta should be 0 )
    // Moreover modularity of a single vertex is zero by default
    var oldCom = List[Long](1L)

    // Foreach community inside the bundle
    for (com <- testBundle) {

      println(s"\n\n\n\n\nStep $com\n")
      println(s"Delta: ${com.filterNot(oldCom.contains(_))}")

      // Take only those Id which represent the delta since last computation
      com.filterNot(oldCom.contains(_)).foreach(id => {

        // Take the reference of that vertex. In spite of ".first()" there should always be only one value
        val switchingVertex: myVertex = graph.vertices.filter(v => v._1 == id).values.first()
        println(s"switchingVertex $switchingVertex")
        // CommId of the vertex will be 1, for testing purpose
        val newCom = 1L
        // Count edges inside the old community to be subtracted and count edges inside the new community to be added
        //(OldCommunity, NewCommunity)

        // Tried to optimize by filtering edges throws EmptyCollection error
        //        val edgesChange = graph.triplets.filter(tri => tri.srcAttr == switchingVertex.verId || tri.dstAttr == switchingVertex.verId).map(tri =>
        //          if (tri.srcId == switchingVertex.verId && tri.dstId == switchingVertex.comId) (1, 0) else if (tri.srcId == switchingVertex.verId && tri.dstId == newCom) (0, 1) else
        //            (0, 0)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val edgesChange = graph.triplets.map(tri => {
          if (tri.srcId == switchingVertex.verId && tri.dstAttr.comId == switchingVertex.comId) (1, 0)
          else if (tri.srcId == switchingVertex.verId && tri.dstAttr.comId == newCom) (0, 1)
          else (0, 0)
        }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

        //ToDo fix bug. Non cambia la community al nodo 2, quindi non legge i nuovi archi. Merda
        graph.triplets.map(tri => {
          if (tri.srcId == switchingVertex.verId && tri.dstAttr.comId == switchingVertex.comId) (List(tri), (1, 0))
          else if (tri.srcId == switchingVertex.verId && tri.dstAttr.comId == newCom) (List(tri), (0, 1))
          else (List(), (0, 0))
        }).collect().foreach(println)

        println(s"Tupla archi $edgesChange")

        // Foreach community inside the list, update modularity values in the communityRDD
        val commRDD1 = commRDD.map(c => {
          println(s"Controllo la comunita' $c in relazione al vertice $switchingVertex")
          // If the community is the old one
          if (c.comId == switchingVertex.comId)
            c.removeFromComm(switchingVertex, edgesChange._1, totEdges)

          //Else if the community is the new one
          else if (c.comId == newCom)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          println(s"Comunita' aggiornata $c")
          c
        })
        println(s"Commrdd1")
        commRDD1.collect().foreach(println)
        commRDD = commRDD1
        //        commRDD.count()
      })

      println(s"Stampo tutte le comunita' dopo il cambio di $com \n${commRDD.collect().foreach(println)}\nFineComunita'")

      result += s"Modularity of: $com:\t ${
        commRDD.map(c => {
          println(s"c.modularity ${c.modularity}")
          c.modularity
        }).reduce((c, v) => {
          println(s"c + v $c + $v = ${c + v}")
          c + v
        })
      }"

      oldCom = com
      println("ð" * 100 + "\n")
    }
    val endDate = System.currentTimeMillis()
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    commRDD.collect().foreach(println)
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

    //    graph.vertices.foreach(println)
    val primaParte = graph.mapTriplets(trip => if (trip.srcAttr.comId == trip.dstAttr.comId) 1L else 0L).edges.reduce((a, b) => new Edge[Long](0, 0, a.attr + b.attr)).attr / 2

    println(s"Prima parte $primaParte")

    //Lista di funzioni da riga-vertice a valore y da togliere alla mod.
    val functionList = graph.vertices.map(x =>
      List((pay: (VertexId, myVertex)) => if (pay._2.comId == x._2.comId && x._1 != pay._1) {
        -pay._2.degree.toFloat * x._2.degree.toFloat / (2 * totEdges)
      } else 0.0)
    ).reduce((a, b) => a ::: b)
    //Mappando ai vertici una funzione che mappa a tutte le funzioni nella mia lista ogni vertice e sommando tutto
    // a ritroso si ha il risultato
    val secondaParte = graph.vertices.map(ver => functionList.map(f => f(ver)).sum).reduce((a, b) => a + b)

    println(s"Seconda Parte $secondaParte")

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