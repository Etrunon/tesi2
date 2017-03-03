package myThesis

import java.io._
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
      List(1, 2, 3),
      List(1, 2),
      List(1),
      List()
      //          List(1, 2, 3, 4),
      //          List(1, 2, 3, 4, 5),
      //          List(1, 2, 3, 4, 5, 6),
      //          List(1, 2, 3, 4, 5, 6, 14),
      //          List(1, 2, 3, 4, 5, 6, 14, 15),
      //          List(1, 2, 3, 4, 5, 6, 14, 15, 30),
      //          List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28),
      //          List(1, 2, 3, 4, 5, 6, 14, 15, 30, 28, 23, 8)
    )

    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[3]")
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
    saveSingleLine(s"File used $edgeFile\n")

    // create the graph from the file and add util data: (degree, commId)
    val graphLoaded: Graph[(Long, Long), Long] = readGraph(sc, edgeFile)

    //    val res1 = testBundleTestModularity(testBundle, graphLoaded)
    //        val res2 = testBundleTestMigration(testBundle, graphLoaded, sc)
    //    val res3 = testBundleDeltasTestMigration(testBundle, graphLoaded)
    val res4 = strategicCommunityFinder(graphLoaded, sc)

    //    saveResultBulk(res1)
    //        saveResultBulk(res2)
    //    saveResultBulk(res3)
    saveResultBulk(res4)

    //    res1.foreach(println)
    //        res2.foreach(println)
    //    res3.foreach(println)
    res4.foreach(println)

    // Line to make program stop and being able to view SparkWebUI
    //    readInt()
  }

  /**
    * Function that given a list of possible operations returns a list of compatible operation which should result in the maximized gain
    * It uses dynamic programming with memoization (in the memoization and mapIndex parameters)
    *
    * @param list        list of operation (vertex to change, community toward change it)
    * @param banSet      to be set Set(), on external call set of banned communities
    * @param memoization to be set mutable.Map() on external call
    * @param mapIndex    to be set at 0 on external call
    * @return
    */
  def dynamicScheduler(list: List[(myVertex, Community)], banSet: Set[Long], memoization: mutable.Map[Long, List[(myVertex, Community)]], mapIndex: Long): List[(myVertex, Community)] = {

    var finale: List[(myVertex, Community)] = List()
    println(s"to be scheduled List $list")
    list match {
      case head :: Nil =>
        if (!(banSet.contains(head._1.comId) || banSet.contains(head._2.comId))) {
          finale = List(head)
        }
        else {
          finale = List()
        }
      case head :: tail =>
        // Else if the operation is banned return possible operation without this
        if (banSet.contains(head._1.comId) || banSet.contains(head._2.comId)) {
          finale = dynamicScheduler(tail, banSet, memoization, mapIndex + 1)
        }
        //If current operation is not banned
        else {
          // Compute the values with current and without
          val withFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex, null) == null) {
            val x = List(head) ::: dynamicScheduler(tail, banSet ++ Set(head._2.comId, head._1.comId), memoization, mapIndex + 1)
            memoization(mapIndex) = x
            x
          } else
            memoization.getOrElse(mapIndex, null)

          val withoutFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex + 1, null) == null) {
            val x = dynamicScheduler(tail, banSet, memoization, mapIndex + 2)
            memoization(mapIndex + 1) = x
            x
          } else
            memoization.getOrElse(mapIndex + 1, null)

          // Whichever is bigger is returned
          if (withFirst.map(x => x._2.modularity).sum > withoutFirst.map(x => x._2.modularity).sum) {
            finale = withFirst
          }
          else {
            finale = withoutFirst
          }
        }
      case Nil =>
    }
    finale
  }

  /**
    * Functions to prune leaves recursively from the graph. It takes as input a graph "undirected" (each edge has its symmetric) and prune the present leaves
    *
    * @param graph to be pruned
    * @param sc    spark context, to broadcast a filterlist
    * @return
    */
  def pruneLeaves(graph: Graph[myVertex, Long], sc: SparkContext): Graph[myVertex, Long] = {
    var removed = false
    var graph2 = graph

    do {
      removed = false
      val leaves = graph2.degrees.filter(v => v._2 / 2 <= 1)
      if (leaves.count() > 0) {
        val leavesBC = sc.broadcast(leaves.map(v => Set(v._1)).reduce((a, b) => a ++ b))
        removed = true
        val newVertices = graph2.vertices.filter(v => !leavesBC.value.contains(v._2.verId))
        val newEdges = graph2.edges.filter(e => !leavesBC.value.contains(e.srcId) && !leavesBC.value.contains(e.dstId))

        graph2 = Graph(newVertices, newEdges)
      }
    } while (removed)
    graph2
  }

  def getVertexFromComm(commRDD: RDD[Community], sc: SparkContext): RDD[(Long, myVertex)] = {
    val chee = commRDD.map(c => c.members.toList).reduce((a, b) => a ::: b)
    sc.parallelize(chee.map(v => (v.verId, v)))
  }

  def getVertexTriplets(vertices: RDD[(Long, myVertex)], triplets: RDD[myTriplet]): RDD[(myVertex, myVertex)] = {
    triplets.map(t => (t.scrId, t)).join(vertices).map(j => (j._2._1.dstId, j._2._2)).join(vertices).map(j => (j._2._1, j._2._2))
  }

  def strategicCommunityFinder(graphLoaded: Graph[(Long, Long), Long], sc: SparkContext): ListBuffer[String] = {
    val initDate = System.currentTimeMillis
    val degrees = graphLoaded.degrees
    val result = ListBuffer[String]()
    result += "Strategic Community Finder"

    // Generate a graph with the correct formatting
    val tmpGraph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(degOpt.getOrElse(0).toLong / 2, id, id) }
    val graph = pruneLeaves(tmpGraph, sc)

    println(s"Vertices")
    graph.vertices.collect().foreach(println)

    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    var vertexRDD: RDD[(Long, myVertex)] = getVertexFromComm(commRDD, sc)

    //    println(s"\n\nComunita' divise per membri")
    //    commRDD.map(c => c.members).collect().foreach(println)
    println(s"\n\nVertici")
    vertexRDD.collect().foreach(println)
    var updated = false

    var cycle = 0L
    do {
      println(s"Cycle $cycle")
      //Look through the frontier of each community and count how many edges they receive from which vertex
      val commNeighCounts = graph.triplets.map(tri => (tri.srcAttr.verId, tri)).join(vertexRDD).map(j => {
        val dstId: Long = j._2._1.dstAttr.verId
        val triplet: EdgeTriplet[myVertex, Long] = j._2._1
        val srcVertex: myVertex = j._2._2
        (dstId, (triplet, srcVertex))
      }).join(vertexRDD).map(k => {
        val srcVer: myVertex = k._2._1._2
        val dstVer: myVertex = k._2._2
        (srcVer, dstVer)
      }).groupBy(tri => tri._2.comId).map(group => {
        //Count how many edges are from a vertex to the same community
        val currComm: Long = group._1
        val edgeCount = group._2.filterNot(g => g._1.comId == currComm).groupBy(dver => dver._1.verId).map(srcGroup => {
          val countedGroup = srcGroup._2.map(g => (g._1, 1L)).reduce((a, b) => (a._1, a._2 + b._2))
          countedGroup
        })
        (currComm, edgeCount)
      })
      //Get the updated triplet objects
      val triplets: RDD[myTriplet] = graph.triplets.map(v => new myTriplet(v.srcAttr.verId, v.dstAttr.verId))
      val updatedTriplets = getVertexTriplets(vertexRDD, triplets)

      // Get the incoming frontier of each community listing each neighbour and how many times it comes into me
      // Group by destination Community
      val incomingCommEdges = updatedTriplets.groupBy(t => t._2.comId).map(g => {

        // Take the list of incoming edges and group it by SourceVertex Id and produce the
        val incomingEdgeList = g._2.groupBy(vv => vv._1.verId).map(x => x._2.map(vv => ((vv._1.verId, vv._1), 1L)).reduce((a, b) => {
          val verId: Long = a._1._1
          val ver: myVertex = a._1._2
          ((verId, ver), a._2 + b._2)
        }))

        val incomingEdgeMap: Map[(Long, myVertex), Long] = incomingEdgeList
        val baseNode = g._2.head._2
        (baseNode.comId, (baseNode, incomingEdgeMap))
      })

      println(s"Incoming Edges")
      incomingCommEdges.collect().foreach(println)
      //      incominCommEdges.map(ie => (ie._1.comId, ))


      //      println(s"\n\n\nCommNeigh")
      //      println(s"${commNeighCounts.collect().foreach(println)}")

      // Espose the index of community to operate a join
      val indexedComm = commRDD.map(co => (co.comId, co))
      // Compute the improve in modularity from joining each of the vertex of the frontier

      val reworkedImprovements = incomingCommEdges.join(indexedComm).map(j => {
        (j._2._2, j._2._1)
      }).map(cmap => {
        val community = cmap._1
        val map = cmap._2._2
        val dstver = cmap._2._1

        map.map(elem => (community, elem._1._2, community.potentialVertexGain(elem._1._2, elem._2, totEdges) + elem._1._2.potentialLoss)).toList
      }) //.reduce((a, b) => a ::: b).filter(value => value._3 > 0.0)

      //      println(reworkedImprovements)
      reworkedImprovements.collect().foreach(println)

      val finalImprovement = commNeighCounts.join(indexedComm).map(union => {
        union._2._1.map(ver => {
          //          println(s"Potential gain + potential loss ver ${ver._1}" +
          //            s"\n${union._2._2.potentialVertexGain(ver._1, ver._2, totEdges)} + ${ver._1.potentialLoss}" +
          //            s"\n ${union._2._2.potentialVertexGain(ver._1, ver._2, totEdges) + ver._1.potentialLoss}")
          (union._2._2.potentialVertexGain(ver._1, ver._2, totEdges) + ver._1.potentialLoss, List[(myVertex, Community)]((ver._1, union._2._2)))
        })
      }).reduce((a, b) => {
        (a.keySet ++ b.keySet).map(i => (i, a.getOrElse(i, List[(myVertex, Community)]()) ::: b.getOrElse(i, List[(myVertex, Community)]()))).toMap
      }) //.filter(imp => {
      //        imp._1 > 0.0
      //      })

      println(s"\n\nFinalImprovement")
      finalImprovement.foreach(println)
      System.exit(14)

      // ToDo finire di ricalcolare il gain utilizzando i vertici correnti e non quelli fissi grafo
      // ToDo scoprire se questo sopra fixa la modularity < -1
      // ToDo Finire di mergiare le comunita'
      // ToDo Scoprire se questo sopra fixa il problema del grafo a cappello stellato
      // ToDo scoprire se il problema dei nodi duplicati era dovuto all'uso dei vertici non aggiornati del grafo
      // ToDo refactorare in maniera piu' sensata... sta collassando l'editor qui dentro
      // ToDo andare a piangere in un angolo

      val schedule = ListBuffer[(myVertex, Community)]()
      finalImprovement.keySet.toList.sorted.reverse.foreach(k => {

        val curr = finalImprovement.get(k)
        val result = mutable.ListBuffer[(myVertex, Community)]()
        curr.foreach(change => {
          change.foreach(tuple => {
            tuple._2.modularity = k
            result += tuple
          })
        })
        schedule.++=(result)
      })

      //      println(s"\n\nSchedule")
      //      schedule.foreach(println)
      val scheduleOptimized: List[(myVertex, Community)] = dynamicScheduler(schedule.toList, Set(), mutable.Map(), 0L)

      println(s"\n\nSchedule Optimized")
      scheduleOptimized.foreach(println)

      val scheduleWithPartingEdges = scheduleOptimized.map(sc => {
        graph.triplets.map(tri => {
          if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._1.comId)
            (sc._1, sc._2, (1L, 0L))
          else if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._2.comId)
            (sc._1, sc._2, (0L, 1L))
          else
            (sc._1, sc._2, (0L, 0L))
        }).reduce((a, b) => (a._1, a._2, (a._3._1 + b._3._1, a._3._2 + b._3._2)))
      })

      if (schedule.length < 1) {

        updated = false
      }
      else {
        updated = true
        //        println(s"\n\nBefore applying deltas")
        //        commRDD.collect().foreach(println)

        commRDD = changeListDelta(graph, commRDD, sc.parallelize(scheduleWithPartingEdges), totEdges)

        //        println(s"\n\nCome esce dall'aggiornamento")
        //        commRDD.collect().foreach(println)
        //        println(s"\n\nTolgo le comunita' vuote")
        commRDD = commRDD.map(c => if (c.members.length < 1) null else c).filter(_ != null).distinct()

        println(s"\n\nAfter Computation")
        println(s"Total Modularity: ${commRDD.map(c => c.modularity).sum()}\n")
        commRDD.collect().foreach(println)

        vertexRDD = commRDD.flatMap(c => c.members).map(v => (v.verId, v))
      }

      //Prova a mergiare le community
      // Per ogni comunita' guardo quanti link ha in uscita sulle altre comunita'
      // (comId, Map( (comId, #link, Map(vertice -> #link)))
      //            val exposedSrcComm = graph.triplets.map(tri => (tri.srcAttr.comId, tri))
      //            val exposedDstComm = graph.triplets.map(tri => (tri.dstAttr.comId, tri))
      //            val exposedComm = commRDD.map(c => (c.comId, c))
      //
      //            val joinableSrc = exposedSrcComm.join(exposedComm).map(comTri => ((comTri._2._1.srcAttr.verId, comTri._2._1.dstAttr.verId), comTri._2))
      //            val joinableDst = exposedDstComm.join(exposedComm).map(comTri => ((comTri._2._1.srcAttr.verId, comTri._2._1.dstAttr.verId), comTri._2))
      //
      //            val equindi = joinableSrc.join(joinableDst).map(comTriCom => {
      //              val srcCom: Community = comTriCom._2._1._2
      //              val triplet: EdgeTriplet[myVertex, Long] = comTriCom._2._1._1
      //              val dstCom: Community = comTriCom._2._2._2
      //
      //              (srcCom, triplet, dstCom)
      //            }).groupBy(comTriCom => comTriCom._1).map(x => {
      //              val commResult = x._2.map(outGoing => {
      //                //          List(outGoing._3, outGoing._2.srcAttr)
      //                List((outGoing._3.comId, outGoing._2.srcAttr.verId))
      //              }).reduce((a, b) => a ::: b)
      //              List(x._1, commResult)
      //            }).reduce((a, b) => {
      //              a ::: b
      //            })
      //
      //            //      equindi.collect().foreach(println)
      //            equindi.foreach(println)

      println("\n" + s"x" * 175 + "\n")
      cycle += 1
    } while (updated)

    result += "Final score"
    commRDD.collect().foreach(c => result += c.toString)

    val endDate = System.currentTimeMillis
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }

  /**
    * Function that given a graph object, an RDD of Communities, an RDD of changes and the number of edges in the graph
    * applies those changes one by one returning the modified Community RDD.
    * The list of change can be described as (myVertex, Community, (Long, Long)):
    * the vertex to change, towards that community bringing out this amount of edges from its current community (first member of the tuple)
    * and bringing that amount of edges to the new one (second member of the tuple)
    *
    * @param graph      graph
    * @param commRDD    rdd of communities
    * @param changeList list of scheduled changes
    * @param totEdges   graph constant
    * @return updated Community
    */
  def changeListDelta(graph: Graph[myVertex, VertexId], commRDD: RDD[Community], changeList: RDD[(myVertex, Community, (Long, Long))], totEdges: Long): RDD[Community] = {

    // Select the community and the node to which add and the amount of new edges
    val addChangeCom = changeList.map(cl => (cl._2.comId, (cl._1, cl._3._2)))
    // Select the community from which the node has to be removed and the amount of edges that it brings out
    val removeChangeCom = changeList.map(cl => (cl._1.comId, (cl._1, cl._3._1)))

    // Select the communities and expose the index
    val exposedComm = commRDD.map(c => (c.comId, c))
    //    exposedComm.collect().foreach(println)

    //    println(s"\n\nAddChangeCom")
    //    addChangeCom.map(ac => {
    //      s"Comm ${ac._1} add ${ac._2._2}"
    //    }).collect().foreach(println)
    //    println(s"\n\nRemoveChangeComm")
    //    removeChangeCom.map(rc => {
    //      s"Comm ${rc._1} remove ${rc._2._2}"
    //    }).collect().foreach(println)
    //    println(s"\n\nExposedComm")
    //    exposedComm.collect().foreach(println)

    val joined: RDD[(Community, (myVertex, Long), (myVertex, Long))] = exposedComm.fullOuterJoin(addChangeCom).map(j => {
      val index: Long = j._1
      val community: Community = j._2._1.orNull
      val add: (myVertex, Long) = j._2._2.orNull
      (index, (community, add))
    }).fullOuterJoin(removeChangeCom).map(j => {
      val community: Community = j._2._1.orNull._1
      val add: (myVertex, Long) = j._2._1.orNull._2
      val remove: (myVertex, Long) = j._2._2.orNull
      (community, add, remove)
    })

    val newCommRDD = joined.map(j => {
      val comm = j._1
      val add = j._2
      val remove = j._3

      if (add != null)
        comm.addToComm(add._1, add._2, totEdges)
      if (remove != null)
        comm.removeFromComm(remove._1, remove._2, totEdges)

      comm
    })

    //    println(s"Risultato finale")
    //    newCommRDD.collect().foreach(println)

    newCommRDD
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

  def testBundleTestMigration(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long], sc: SparkContext): ListBuffer[String] = {
    //Timed execution
    val initDate = System.currentTimeMillis
    val degrees = graphLoaded.degrees
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
    var oldCom: List[Long] = List()
    // CommId of the vertex will be 1, for testing purpose
    val addingComm = new Community(100L, 0.0, ListBuffer())
    val removingComm = new Community(200L, 0.0, ListBuffer())
    commRDD = commRDD.union(sc.parallelize(List(addingComm, removingComm))).distinct()
    // Foreach community inside the bundle
    for (com <- testBundle) {
      println(s"\n\nCommunity status update")
      commRDD.collect().foreach(println)

      val innerTimeInit = System.currentTimeMillis()

      // Take only those Id were added to last community
      com.filterNot(oldCom.contains(_)).foreach(id => {
        println(s"Add ver $id to com ${addingComm.comId}")

        // Take the reference of that vertex. In spite of ".first()" there should always be only one value
        val switchingVertex: myVertex = graph.vertices.filter(v => v._1 == id).values.first()
        println(s"Switching Vertex $switchingVertex")

        // Count edges inside the old community to be subtracted and count edges inside the new community to be added
        //(OldCommunity, NewCommunity)
        val oldComPointer = commRDD.filter(c => c.comId == switchingVertex.comId).first()
        val newComPointer = commRDD.filter(c => c.comId == addingComm.comId).first()
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
          else if (c.comId == addingComm.comId)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          c
        })
      })

      // Take only those Id which were removed from last community
      oldCom.filterNot(com.contains(_)).foreach(id => {
        println(s"Removing ver $id towards com ${removingComm.comId}")

        // Take the reference of that vertex. In spite of ".first()" there should always be only one value
        val switchingVertex: myVertex = graph.vertices.filter(v => v._1 == id).values.first()
        println(s"Switching Vertex $switchingVertex")

        // Count edges inside the old community to be subtracted and count edges inside the new community to be added
        //(OldCommunity, NewCommunity)
        val oldComPointer = commRDD.filter(c => c.comId == switchingVertex.comId).first()
        val newComPointer = commRDD.filter(c => c.comId == removingComm.comId).first()
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
          else if (c.comId == removingComm.comId)
            c.addToComm(switchingVertex, edgesChange._2, totEdges)
          c
        })
      })

      println(s"After UPDATE")
      commRDD.collect().foreach(println)

      val innerTimeEnd = System.currentTimeMillis()
      result += s"Time: ${innerTimeEnd - innerTimeInit}\t Modularity of: $com:\t ${commRDD.map(c => c.modularity).reduce((c, v) => c + v)}"
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

}