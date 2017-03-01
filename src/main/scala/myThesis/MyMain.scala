package myThesis

import java.io.{File, _}
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
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
    saveSingleLine(s"File used $edgeFile\n")

    // create the graph from the file and add util data: (degree, commId)
    val graphLoaded: Graph[(Long, Long), Long] = readGraph(sc, edgeFile)

    //    val res1 = testBundleTestModularity(testBundle, graphLoaded)
    //    val res2 = testBundleTestMigration(testBundle, graphLoaded)
    //    val res3 = testBundleDeltasTestMigration(testBundle, graphLoaded)
    val res4 = strategicCommunityFinder(graphLoaded)

    //    saveResultBulk(res1)
    //    saveResultBulk(res2)
    //    saveResultBulk(res3)
    saveResultBulk(res4)

    //    res1.foreach(println)
    //    res2.foreach(println)
    //    res3.foreach(println)
    res4.foreach(println)

    // Line to make program stop and being able to view SparkWebUI
    //    readInt()
  }

  def strategicCommunityFinder(graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
    val initDate = System.currentTimeMillis
    val degrees = graphLoaded.degrees
    val result = ListBuffer[String]()
    result += "Strategic Community Finder"

    // Generate a graph with the correct formatting
    var graph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(degOpt.getOrElse(0).toLong / 2, id, id) }
    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    //    val tmpComm = List[Long](1, 2, 3, 4)
    //    val tmpGraph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) =>
    //      new myVertex(degOpt.getOrElse(0).toLong / 2, if (tmpComm.contains(id)) 1L else id, id)
    //    }

    // Takes a graph triplets rdd and return a map of communities with their frontier neighbours and how many times they link with each neighbour
    val commNeighCounts = graph.triplets.groupBy(tri => {
      tri.dstAttr.comId
    }).map(groupedComm => {
      (groupedComm._1, groupedComm._2.filterNot(f => f.srcAttr.comId == groupedComm._1).groupBy(tri => tri.srcAttr.verId).map(groupedTriplets => {
        groupedTriplets._2.map(tri => {
          (tri.srcAttr, 1L)
        }).reduce((a, b) => (a._1, a._2 + b._2))
      }))
    })

    val indexedComm = commRDD.map(co => (co.comId, co))
    val finalImprovement = commNeighCounts.join(indexedComm).map(union => {
      union._2._1.map(ver => {
        (union._2._2.scoreDifference(ver._1, ver._2, totEdges), List[(myVertex, Community)]((ver._1, union._2._2)))
      })
    }).reduce((a, b) => {
      (a.keySet ++ b.keySet).map(i => (i, a.getOrElse(i, List[(myVertex, Community)]()) ::: b.getOrElse(i, List[(myVertex, Community)]()))).toMap
    })

    val bannList = mutable.ListBuffer[Long]()
    val schedule = ListBuffer[(myVertex, Community)]()
    finalImprovement.keySet.toList.sorted.reverse.foreach(k => {

      val curr = finalImprovement.get(k)
      val result = mutable.ListBuffer[(myVertex, Community)]()
      curr.map(change => {
        change.foreach(tuple => {
          // Add (node-> comm) to the schedule. Also symmetric operations
          if (!bannList.contains(tuple._1.verId) && !bannList.contains(tuple._2.comId)) {
            result += tuple
            bannList += (tuple._1.verId, tuple._2.comId)
          }
        })
        schedule.++=(result)
      })
    })

    //    schedule.foreach(v => println(s"vertex ${v._1} to comm ${v._2}"))

    commRDD = changeListDelta(graph, commRDD, schedule, totEdges)

    commRDD.collect().foreach(println)

    val endDate = System.currentTimeMillis
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }


  def testBundleTestMigration(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
    //Timed execution
    val initDate = System.currentTimeMillis
    val degrees = graphLoaded.degrees
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
      val innerTimeInit = System.currentTimeMillis()

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
        commRDD = commRDD.map(c => {
          // If the community is the old one
          if (c.comId == switchingVertex.comId)
            c.removeFromComm(switchingVertex, edgesChange._1, totEdges)

          //Else if the community is the new one
          else if (c.comId == newCom)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          c
        })
      })

      val innerTimeEnd = System.currentTimeMillis()
      result += s"Time: ${innerTimeEnd - innerTimeInit}\t Modularity of: $com:\t ${commRDD.map(c => c.modularity).reduce((c, v) => c + v)}"
      oldCom = com
    }
    val endDate = System.currentTimeMillis()
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }

  def changeListDelta(graph: Graph[myVertex, VertexId], commRDD: RDD[Community], changeList: ListBuffer[(myVertex, Community)], totEdges: Long): RDD[Community] = {
    if (changeList.length < 1)
      commRDD
    else {
      var commRDD1: RDD[Community] = null
      changeList.foreach(change => {
        val switchingVertex: myVertex = change._1

        // Count edges inside the old community to be subtracted and count edges inside the new community to be added
        //(OldCommunity, NewCommunity)
        val oldComPointer = commRDD.filter(c => c.comId == switchingVertex.comId).first()
        val newComPointer = change._2
        val edgesChange = graph.triplets.filter(tri => tri.srcId == switchingVertex.verId).map(tri => tri.dstAttr).map(dstId => {
          if (oldComPointer.members.contains(dstId)) (1, 0)
          else if (newComPointer.members.contains(dstId)) (0, 1)
          else (0, 0)
        }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

        // Foreach community inside the list, update modularity values in the communityRDD
        commRDD1 = commRDD.map(c => {
          // If the community is the old one
          if (c.comId == switchingVertex.comId)
            c.removeFromComm(switchingVertex, edgesChange._1, totEdges)

          //Else if the community is the new one
          else if (c.comId == newComPointer.comId)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          c
        })
        commRDD1
      })
      commRDD1
    }
  }

  /*
    def testBundleDeltasTestMigration(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
      //Timed execution
      val initDate = System.currentTimeMillis
      val degrees = graphLoaded.degrees
      val result = ListBuffer[String]()
      result += "Deltas Migration"

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
        val innerTimeInit = System.currentTimeMillis()

        val changeList = com.filterNot(oldCom.contains(_)).map(id => (id, newCom))
        commRDD = changeListDelta(graph, commRDD, changeList, totEdges)

        commRDD.collect().foreach(println)

        // Take only those Id which represent the delta since last computation

        val innerTimeEnd = System.currentTimeMillis()
        val compModularity = commRDD.map(c => c.modularity).reduce((c, v) => c + v)
        result += s"Time: ${innerTimeEnd - innerTimeInit}\t Modularity of: $com:\t $compModularity"
        oldCom = com
      }
      val endDate = System.currentTimeMillis()
      result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
      result
    }
  */

  def testBundleTestModularity(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long]): ListBuffer[String] = {
    val initDate = System.currentTimeMillis()

    val degrees = graphLoaded.degrees.cache()
    val result = ListBuffer[String]()
    result += "Whole Computation"

    for (tmpComm <- testBundle) {
      val innerTimeInit = System.currentTimeMillis()
      // (Degree, CommId)
      val tmpGraph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) =>
        new myVertex(degOpt.getOrElse(0).toLong / 2, if (tmpComm.contains(id)) 1L else id, id)
      }
      //      println(s"Modularity of $tmpComm:\t ${modularity(graph)}")
      val comModularity = modularity(tmpGraph)
      val innerTimeEnd = System.currentTimeMillis()
      val s = s"Time: ${innerTimeEnd - innerTimeInit}\tModularity of $tmpComm:\t $comModularity"
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
    val pw = new PrintWriter(new FileOutputStream(System.getProperty("output_path") + "/Result.txt", true))
    result.foreach(line => pw.append(line + "\n"))
    pw.close()
  }

  def saveSingleLine(line: String): Unit = {
    val pw = new PrintWriter(new FileOutputStream(System.getProperty("output_path") + "/Result.txt", true))
    pw.append(line + "\n")
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