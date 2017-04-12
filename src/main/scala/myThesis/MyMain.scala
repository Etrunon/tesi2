package myThesis

import java.io._
import java.util.Date

import myThesis.UtilityFunctions.{getVertexFromComm, loadAndPrepareGraph, saveResultBulk, saveSingleLine}
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
  private var conf: SparkConf = _
  private var sc: SparkContext = _
  // Sets source file
  val edgeFile = "RunData/Input/processed_mini1.csv"
  // Sets output folder
  val outputPath: String = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm:ss").format(new Date())
  val outputDir = new File(outputPath)
  outputDir.mkdirs()
  System.setProperty("output_path", outputPath)
  saveSingleLine(s"File used $edgeFile\n")
  val testBundle: List[List[Long]] = List[List[Long]](
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
  val timesToRepeat = 10

  def initContext(): Unit = {
    conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
    sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    initContext()
    val graph = loadAndPrepareGraph(edgeFile, sc)

    graph.vertices.collect().foreach(println)

    //    val res1 = testBundleTestModularity(testBundle, graphLoaded)
    //    val res2 = testBundleTestMigration(testBundle, graphLoaded)
    //    val res3 = testBundleDeltasTestMigration(testBundle, graphLoaded)
    //    val res4 = strategicCommunityFinder(graphLoaded)
    //    val res5 = greedyFinderNeighNeigh(graphLoaded)
    //    saveResultBulk(res1)
    //    saveResultBulk(res2)
    //    saveResultBulk(res3)
    //    saveResultBulk(res4)
    //    saveResultBulk(res5)

    //    res1.foreach(println)
    //    res2.foreach(println)
    //    res3.foreach(println)
    //    res4.foreach(println)
    //    res5.foreach(println)

    var executionCounter = 0
    val totalsBuffer: ListBuffer[String] = ListBuffer()

    for (_ <- 0 to timesToRepeat) {
      //      val res5 = greedyFinderNeighNeigh(graph)
      val foundResult = strategicCommunityFinder(graph, -1, sc)

      var result: ListBuffer[String] = ListBuffer("Final score")
      result += "$" * 200
      result += "$" * 200
      result += "$" * 200
      result += "$" * 200
      result = result ++ foundResult._2
      result += "Total modularity:" + foundResult._1.map(c => c.modularity).sum().toString

      //%%%%%%% Total
      totalsBuffer += s"\n!&(/£&£%\nExecution $executionCounter"
      totalsBuffer ++= foundResult._2
      totalsBuffer += "Total modularity:" + foundResult._1.map(c => c.modularity).sum().toString
      executionCounter += 1

      saveResultBulk(result)
      result.foreach(println)
    }

    totalsBuffer.foreach(println)
    // Line to make program stop and being able to view SparkWebUI
    //    readInt()
  }

  def greedyFinderNeighNeigh(graph: Graph[myVertex, Long]): ListBuffer[String] = {
    val initDate = System.currentTimeMillis
    val result = ListBuffer[String]()
    result += "\nStrategic Community Finder V2 (Neighbours'neighbours)"

    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, ListBuffer(ver._2), 0.0))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    var vertexRDD: RDD[(Long, myVertex)] = getVertexFromComm(commRDD, sc)
    val triplets: RDD[myTriplet] = graph.triplets.map(v => new myTriplet(v.srcAttr.verId, v.dstAttr.verId))

    var updated = false

    //    commRDD.collect().foreach(println)

    var cycle = 0L
    do {
      println(s"£££££££££££££££££££££££££££££££££ Cycle $cycle £££££££££££££££££££££££££££££££££££")

      val updatedTriplets = getVertexTriplets(vertexRDD, triplets)

      // Get the rdd containing each source node with the list of its neighbours which are in a different community
      val listNeighbours = updatedTriplets.groupBy(tri => tri._1.verId).map(tri => {
        // Map the list of neighbours so that it contains only those of different community
        val diffCommNeigh = tri._2.filter(t => t._1.comId != t._2.comId)
        if (diffCommNeigh.nonEmpty) {
          val listNeighbours = diffCommNeigh.map(t => List(t._2)).reduce((a, b) => a ::: b)
          (listNeighbours, Map[myVertex, List[myVertex]](tri._2.head._1 -> listNeighbours))
        } else {
          null
        }
        // Map of (sourceNeighbour -> its neighbours(of diff. comm.) )
      }).filter(a => a != null).collect()

      //      println(s"List neighbours")
      //      listNeighbours.foreach(println)

      val bcListNeighbour = sc.broadcast(listNeighbours)

      val neighboursNeighbours = vertexRDD.map(v => {
        val neighNeigh = mutable.Map[myVertex, List[myVertex]]()
        bcListNeighbour.value.foreach(ln => {
          if (ln._1.contains(v._2))
            neighNeigh ++= ln._2
        })
        (v._2.comId, List(v._2), neighNeigh)
      }).groupBy(t => t._1).map(t => {

        val x = t._2.map(e => e._3).reduce((a, b) => a ++ b)
        (t._1, x)
      })

      val exposedComm = commRDD.map(c => (c.comId, c))

      val initialSchedule = exposedComm.join(neighboursNeighbours).map(_._2).map(cnn => {
        var initialSchedule: ListBuffer[(myVertex, Community, Double)] = ListBuffer[(myVertex, Community, Double)]()
        cnn._2.foreach(nln => {
          val reachability = (nln._2.intersect(cnn._1.members).size.toDouble + 1.0) / (cnn._1.members.size.toDouble + 1.0)
          if (reachability >= 0.5)
            initialSchedule += ((nln._1, cnn._1, reachability))
        })
        initialSchedule.toList
      }).reduce((a, b) => a ::: b)

      //      initialSchedule.foreach(println)
      //      val optimizedSchedule = dynamicReachablityScheduler(initialSchedule.sortBy(_._3).map(t => (t._1, t._2)).reverse, Set(), mutable.Map(), 0L)
      //      println(s"\n\n\nOptimizedSchedule ${optimizedSchedule.size} of unoptimized ${initialSchedule.size}")
      //      optimizedSchedule.foreach(println)

      //      println(s"Initial schedule " + "1" * 100)
      //      println(initialSchedule)

      var mutSet = Set[Long]()
      val opt2 = ListBuffer[(myVertex, Community)]()

      initialSchedule.groupBy(_._1).flatMap(a => {
        val ver = a._1
        val group = a._2

        val selection = group.reduce((a, b) => if (a._3 > b._3) a else b)
        List((ver, selection))
      })

      initialSchedule.reverse.groupBy(_._3).foreach(g => {
        val tup = Scheduler.dynamicReachablityScheduler(g._2.map(t => (t._1, t._2)), mutSet, mutable.Map(), 0L)
        mutSet = tup._2
        opt2 ++= tup._1
      })


      //      println(s"\n\n\nOptimizedSchedule ${opt2.size} of unoptimized ${initialSchedule.size}")
      //      println(s"Optimized schedule " + "1" * 100)
      //      opt2.foreach(println)


      val scheduleWithPartingEdges = opt2.map(sc => {
        graph.triplets.map(tri => {
          if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._1.comId)
            (sc._1, sc._2, (1L, 0L))
          else if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._2.comId)
            (sc._1, sc._2, (0L, 1L))
          else
            (sc._1, sc._2, (0L, 0L))
        }).reduce((a, b) => (a._1, a._2, (a._3._1 + b._3._1, a._3._2 + b._3._2)))
      })

      if (opt2.length < 1) {

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

        //        println(s"\n\nAfter Computation")
        //        println(s"Total Modularity: ${commRDD.map(c => c.modularity).sum()}\n")
        //        commRDD.collect().foreach(println)

        vertexRDD = commRDD.flatMap(c => c.members).map(v => (v.verId, v))
      }

      cycle += 1
      if (cycle == 3)
        updated = false
      if (cycle % 3 == 0)
        commRDD.collect().foreach(println)
    } while (updated)

    result += "Final score"
    commRDD.collect().foreach(c => result += c.toString)

    val endDate = System.currentTimeMillis
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    result
  }

  def getVertexTriplets(vertices: RDD[(Long, myVertex)], triplets: RDD[myTriplet]): RDD[(myVertex, myVertex)] = {
    triplets.map(t => (t.scrId, t)).join(vertices).map(j => (j._2._1.dstId, j._2._2)).join(vertices).map(j => (j._2._1, j._2._2))
  }

  def findFrontier(graph: Graph[myVertex, Long], vertexRDD: RDD[(Long, myVertex)], commRDD: RDD[Community]): RDD[(Community, Map[myVertex, Long])] = {
    // Expose vertices in the triplet to the point in which we have (myVertex, myVertex) "triplet" obj. This way we can see communities
    val myVertmyVert = graph.triplets.map(tri => (tri.srcAttr.verId, tri)).join(vertexRDD).map(j => {
      val dstId: Long = j._2._1.dstAttr.verId
      val triplet: EdgeTriplet[myVertex, Long] = j._2._1
      val srcVertex: myVertex = j._2._2
      (dstId, (triplet, srcVertex))
    }).join(vertexRDD).map(k => {
      val srcVer: myVertex = k._2._1._2
      val dstVer: myVertex = k._2._2
      (srcVer, dstVer)
    })

    val commNeighCounts = myVertmyVert.groupBy(tri => tri._2.comId).map(group => {

      //Count how many edges are from a vertex to the same community
      val currComm: Long = group._1
      val edgeCount = group._2.filterNot(g => g._1.comId == currComm).groupBy(dver => dver._1.verId).map(srcGroup => {
        val countedGroup = srcGroup._2.map(g => (g._1, 1L)).reduce((a, b) => (a._1, a._2 + b._2))
        countedGroup
      })
      (currComm, edgeCount)
    })

    // Join together the Community obj and its frontier vertex list
    commNeighCounts.join(commRDD.map(c => (c.comId, c))).map(res => (res._2._2, res._2._1))
  }

  def strategicCommunityFinder(graph: Graph[myVertex, Long], maxCycle: Int, sc: SparkContext): (RDD[Community], ListBuffer[String]) = {
    // Set the maximum number of cycles. If less than zero, then set the maximum Long value
    val endCycle: Long = if (maxCycle >= 0) maxCycle else Long.MaxValue
    val initDate = System.currentTimeMillis
    val result = ListBuffer[String]()
    result += "Strategic Community Finder V1 (Neighbours's Modularity)"

    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, ListBuffer(ver._2), 0.0)).cache
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    var vertexRDD: RDD[(Long, myVertex)] = getVertexFromComm(commRDD, sc).cache()

    var updated = false
    var comPrinted = false
    var cycle = 0L
    do {
      comPrinted = false

      result += s"Cycle $cycle"
      //Look through the frontier of each community and count how many edges they receive from which vertex
      val doableOperations = findFrontier(graph, vertexRDD, commRDD).map(caf => {
        val currComm = caf._1
        val currMap = caf._2

        val res = currMap.map(candidate => {
          List[(Community, myVertex, Long, Double)]((currComm, candidate._1, candidate._2, currComm.potentialVertexGain(candidate._1, candidate._2, totEdges)))
        }).reduce((a, b) => a ::: b).filter(elem => {
          //Filter out those operation which improve less than zero
          elem._4 + elem._2.potentialLoss > 0.0
        })

        if (res.nonEmpty) {
          val bestComOperation = res.reduce((a, b) => {
            // Take only the best operation for each community
            val netA = a._4 + a._2.potentialLoss
            val netB = b._4 + b._2.potentialLoss

            if (netA >= netB) a else b
          })

          bestComOperation
        } else
          null
      }).filter(a => a != null).cache

      println(s"\n\tDoableOperations (size ${doableOperations.count})")
      doableOperations.collect().foreach(println)
      println("\n")

      if (doableOperations.count < 1) {
        updated = false
      }
      else {
        updated = true

        // New code without dynamic scheduler
        //ToDo find bug for which there is sometimes a diplicated operation
        val euristicOptimized = doableOperations.mapPartitions(it => {
          //First we map the partitions and find the optimum set of operation in each partition
          if (it.nonEmpty) {
            val list = it.toList
            val forScheduler = list.map(op => (op._2, op._1))
            val optim = Scheduler.dynamicScheduler(forScheduler).map(oop => oop._1.toString + oop._2.toString)
            list.filter(f => optim.contains(f._2.toString + f._1.toString)).toIterator
          } else
            it
        }).map(op => {
          // Then we reduce the result into a global view
          Map(op._2 -> (op._1, op._2.connectingEdges, op._3))
        }).reduce((a, b) => {
          // Teh resulting set of operation is no more optimum but, should be good enough
          (a.keySet ++ b.keySet).map(k => {
            val ka = a.getOrElse(k, null)
            val kb = b.getOrElse(k, null)

            if (ka == null)
              k -> kb
            else if (kb == null)
              k -> ka
            else {
              //              if (ka._1.members.size > kb._1.members.size)
              if (ka._1.modularity > kb._1.modularity)
                k -> ka
              else
                k -> kb
            }
          }).toMap
        }).toList.map(op => (op._1, op._2._1, (op._2._2, op._2._3)))

        val scheduleWithPartingEdges = sc.parallelize(euristicOptimized).cache()

        println(s"\n\tSchedule with parting Edges (size ${scheduleWithPartingEdges.count})")
        scheduleWithPartingEdges.collect().foreach(println)
        println("\n")

        commRDD = changeListDelta(graph, commRDD, scheduleWithPartingEdges, totEdges).cache
        commRDD = commRDD.map(c => if (c.members.length < 1) null else c).filter(_ != null).distinct()
        vertexRDD = commRDD.flatMap(c => c.members).map(v => (v.verId, v))
      }

      cycle += 1
      println(s"\nCommunity updated")
      commRDD.collect().sortBy(c => c.comId).foreach(println)
      println(s"%%" * 100)
      println(s"%%" * 100)

      println(s"Now we try to merge confining communities")
      val newFrontier = findFrontier(graph, vertexRDD, commRDD)
      val joinedComms = newFrontier.map(c => (c._1.comId, c)).join(commRDD.map(c => (c.comId, c))).map(j => {
        val com1: Community = j._2._1._1
        val com2: Community = j._2._2
        val frontier: Map[myVertex, Long] = j._2._1._2
        (com1, frontier, com2)
      })

      println(s"đ" * 200)
      println(s"đ" * 200)
      println(s"đ" * 200)
      println(s"New Frontier")
      newFrontier.collect().foreach(println)

      val commCommFrontier = newFrontier.map(nf => {
        val comFrontier = nf._2.map(t => Map(t._1.community -> Map(t._1 -> t._2))).reduce((a, b) => {
          (a.keySet ++ b.keySet).map(k => {
            val ka = a.getOrElse(k, null)
            val kb = b.getOrElse(k, null)

            if (ka == null)
              k -> kb
            else if (kb == null)
              k -> ka
            else
              k -> (ka ++ kb)
          }).toMap
        })
        (nf._1, comFrontier)
      }).cache

      println(s"\nComComFrontier: ${commCommFrontier.count}")
      commCommFrontier.collect().foreach(println)
      println(s"d" * 200)

      val doableMerges = commCommFrontier.map(ccf => {
        val curCom = ccf._1
        val curComFrontier = ccf._2
        val gain = curComFrontier.map(neiC => {
          (neiC._1, curCom.potentialCommunityGain(neiC._1, neiC._2, totEdges), curComFrontier)
        }).toList.filter(f => f._2 > 0.0)
        (curCom, gain)
      }).cache

      println(s"DoableMerges: ${doableMerges.count}")
      doableMerges.collect().foreach(println)
      println(s"m" * 200)

      val comCom = doableMerges.map(m => {
        m._2.map(e => (m._1, e._1, e._2, e._3.get(e._1).orNull))
      }).reduce((a, b) => a ::: b)

      println(s"ComCom: ${comCom.length}")
      comCom.foreach(println)
      println(s"ß" * 200)

      val scheduledIndex = sc.broadcast(Scheduler.dynamicMergingScheduler(comCom.map(c => (c._1, c._2))).map(sc => sc._1.toString + sc._2.toString))
      println(s"ScheduledIndex: ${scheduledIndex.value.length}")
      scheduledIndex.value.foreach(println)
      println(s"ß" * 200)

      val scheduledOp = sc.parallelize(comCom.filter(f => scheduledIndex.value.contains(f._1.toString + f._2.toString))).map(op => (op._1.comId, op)).cache

      println(s"ScheduledOp: ${scheduledOp.count}")
      scheduledOp.collect().foreach(println)
      println(s"ß" * 200)

      /*
            commRDD = commRDD.map(c=> (c.comId, c)).leftOuterJoin(scheduledOp).map(cOp => {
              val op = cOp._2._2.orNull
              val com = cOp._2._1

              if (op == null)
                com
              else {
                val otherCom = op._2
                val modImprove = op._3
                val frontier = op._4

                otherCom.members.foreach(memb => com.addToComm((memb, frontier.getOrElse(memb, 0))))
              }
            })
      */


      if (cycle == endCycle) {
        updated = false
      }
    } while (updated)

    commRDD.collect().foreach(l => result += l.toString)

    val endDate = System.currentTimeMillis
    result += s"Execution time: ${(endDate - initDate) / 1000.0}"
    (commRDD, result)
  }

  def lineErrorSeparator(): Unit = {
    println("!" * 200)
    println("!" * 200)
    println("!" * 200)
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
    // Format (cId, (vertex, #archs) )
    // cId destination community Id
    // vertex, the vertex to be moved
    // #archs the number of archs that it brings
    val addChangeCom = changeList.map(cl => (cl._2.comId, (cl._1, cl._3._2)))

    // Select the community from which the node has to be removed and the amount of edges that it brings out
    // Format (cId, (vertex, #archs) )
    // cId departure community Id
    // vertex, the vertex to be moved
    // #archs the number of archs that it brings out
    val removeChangeCom = changeList.map(cl => (cl._1.comId, (cl._1, cl._3._1)))

    // Select the communities and expose the index
    val exposedComm = commRDD.map(c => (c.comId, c))

    val joined: RDD[(Community, (myVertex, Long), (myVertex, Long))] = exposedComm.leftOuterJoin(addChangeCom).map(j => {
      val index: Long = j._1
      val community: Community = j._2._1
      val add: (myVertex, Long) = j._2._2.orNull
      (index, (community, add))
    }).leftOuterJoin(removeChangeCom).map(j => {
      val community: Community = j._2._1._1
      val add: (myVertex, Long) = j._2._1._2
      val remove: (myVertex, Long) = j._2._2.orNull

      (community, add, remove)
    })

    println(s"\nJoinedOperations: ")
    joined.collect().foreach(println)

    val newCommRDD = joined.map(j => {
      val comm = j._1
      val add = j._2
      val remove = j._3

      if (add != null)
        comm.addToComm(add, totEdges)
      if (remove != null)
        comm.removeFromComm(remove, totEdges)

      comm
    })

    newCommRDD
  }

}