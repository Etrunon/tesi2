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
  val timesToRepeat = 0

  def initContext(): Unit = {
    conf = new SparkConf().setAppName("CommTesi2").setMaster("local[3]")
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

    for (_ <- 0 to timesToRepeat) {
      //      val res5 = greedyFinderNeighNeigh(graph)
      val commRDD = strategicCommunityFinder(graph, -1, sc)

      val result: ListBuffer[String] = ListBuffer("Final score")
      commRDD.collect().foreach(c => result += c.toString)


      saveResultBulk(result)
      result.foreach(println)
    }
    // Line to make program stop and being able to view SparkWebUI
    //    readInt()
  }

  def greedyFinderNeighNeigh(graph: Graph[myVertex, Long]): ListBuffer[String] = {
    val initDate = System.currentTimeMillis
    val result = ListBuffer[String]()
    result += "\nStrategic Community Finder V2 (Neighbours'neighbours)"

    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
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
        val tup = dynamicReachablityScheduler(g._2.map(t => (t._1, t._2)), mutSet, mutable.Map(), 0L)
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

  def dynamicReachablityScheduler(list: List[(myVertex, Community)], banSet: Set[Long], memoization: mutable.Map[Long, List[(myVertex, Community)]], mapIndex: Long): (List[(myVertex, Community)], Set[Long]) = {

    //    println(" - " * mapIndex.toInt + s"BannList: $banSet")
    var finale: List[(myVertex, Community)] = List()
    list match {
      case head :: Nil =>
        if (!(banSet.contains(head._1.comId) || banSet.contains(head._2.comId))) {
          finale = List(head)
        }
        else {
          finale = List()
        }
      case head :: tail =>
        //        println(" - " * mapIndex.toInt + s"Head: $head")
        //        println(" - " * mapIndex.toInt + s"tail: $tail")
        // Else if the operation is banned return possible operation without this
        if (banSet.contains(head._1.comId) || banSet.contains(head._2.comId)) {
          finale = dynamicReachablityScheduler(tail, banSet, memoization, mapIndex + 1L)._1
        }
        //If current operation is not banned
        else {
          // Compute the values with current and without
          val withFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex, null) == null) {
            val x = List(head) ::: dynamicReachablityScheduler(tail, banSet ++ Set(head._2.comId, head._1.comId), memoization, mapIndex + 1L)._1
            memoization(mapIndex) = x
            x
          } else
            memoization.getOrElse(mapIndex, null)

          val withoutFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex + 1L, null) == null) {
            val x = dynamicReachablityScheduler(tail, banSet, memoization, mapIndex + 2L)._1
            memoization(mapIndex + 1L) = x
            x
          } else
            memoization.getOrElse(mapIndex + 1L, null)

          // Whichever is bigger is returned
          //          println(" - " * mapIndex.toInt + s"${withFirst.size > withoutFirst.size} ${withFirst.size} > ${withoutFirst.size}")
          if (withFirst.size >= withoutFirst.size) {
            finale = withFirst
          }
          else {
            finale = withoutFirst
          }
        }
      case Nil =>
    }
    //    println(" - " * mapIndex.toInt + s"finale: $finale")
    (finale, banSet)
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




  def getVertexTriplets(vertices: RDD[(Long, myVertex)], triplets: RDD[myTriplet]): RDD[(myVertex, myVertex)] = {
    triplets.map(t => (t.scrId, t)).join(vertices).map(j => (j._2._1.dstId, j._2._2)).join(vertices).map(j => (j._2._1, j._2._2))
  }

  def strategicCommunityFinder(graph: Graph[myVertex, Long], maxCycle: Int, sc: SparkContext): RDD[Community] = {
    val endCycle: Long = if (maxCycle >= 0) maxCycle else Long.MaxValue
    val initDate = System.currentTimeMillis
    val result = ListBuffer[String]()
    result += "\nStrategic Community Finder V1 (Neighbours's Modularity)"

    //    println(s"Vertices")
    //    graph.vertices.collect().foreach(println)

    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, 0.0, ListBuffer(ver._2)))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    var vertexRDD: RDD[(Long, myVertex)] = getVertexFromComm(commRDD, sc)
    val triplets: RDD[myTriplet] = graph.triplets.map(v => new myTriplet(v.srcAttr.verId, v.dstAttr.verId))
    //    println(s"\n\nComunita' divise per membri")
    //    commRDD.map(c => c.members).collect().foreach(println)
    //    println(s"\n\nVertici")
    //    vertexRDD.collect().foreach(println)

    var updated = false
    var comPrinted = false
    var cycle = 0L
    do {
      comPrinted = false

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
      val updatedTriplets = getVertexTriplets(vertexRDD, triplets)

      // Get the incoming frontier for each community listing each neighbour and how many times it comes into me
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

      // Espose the index of community to operate a join
      val indexedComm = commRDD.map(co => (co.comId, co))

      // Compute the improve in modularity from joining each of the vertex of the frontier
      val reworkedImprovements = incomingCommEdges.join(indexedComm).map(j => {
        (j._2._2, j._2._1)
      }).map(cmap => {
        val community = cmap._1
        val map = cmap._2._2
        val dstver = cmap._2._1

        val eachCommOperation = map.map(elem => (community, elem._1._2, community.potentialVertexGain(elem._1._2, elem._2, totEdges) + elem._1._2.potentialLoss)).toList
        eachCommOperation.map(a => List(a)).reduce((a, b) => {
          if (math.abs(a.head._3 - b.head._3) < math.pow(10, -6)) a ::: b else if (a.head._3 > b.head._3) a else b
        })
      })

      var finalImprovement = reworkedImprovements.map(a => a.map(b => Map(b._3 -> List((b._2, b._1))))).reduce((a, b) => {
        a ::: b
      }).reduce((c, d) => {
        (c.keySet ++ d.keySet).map(i => (i, c.getOrElse(i, List[(myVertex, Community)]()) ::: d.getOrElse(i, List[(myVertex, Community)]()))).toMap
      })

      //      println("reworkedImprovements")
      //      reworkedImprovements.collect().foreach(println)
      //
      //      var finalImprovement = commNeighCounts.join(indexedComm).map(union => {
      //        union._2._1.map(ver => {
      //          (union._2._2.potentialVertexGain(ver._1, ver._2, totEdges) + ver._1.potentialLoss, List[(myVertex, Community)]((ver._1, union._2._2)))
      //        })
      //      }).reduce((a, b) => {
      //        (a.keySet ++ b.keySet).map(i => (i, a.getOrElse(i, List[(myVertex, Community)]()) ::: b.getOrElse(i, List[(myVertex, Community)]()))).toMap
      //      })

      //      println(s"\tGreedy1: FinalImpr. = ${finalImprovement.size}")
      //      finalImprovement = finalImprovement.filter(imp => {
      //        imp._1 > 0.0
      //      })
      //      println(s"\tGreedy1: FinalImpr. = ${finalImprovement.size}")


      println(s"\n\tFinalImprovement")
      finalImprovement.foreach(println)

      var schedule = ListBuffer[(myVertex, Community)]()
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

      println(s"\n\n\n\tGreedy1: before schedule: ${schedule.length} ")
      //      schedule.sortBy(a=> a._1).foreach(println)
      schedule = schedule.groupBy(op => op._1).map(group => {
        //        println(s"Greedy1: before group")
        //        group._2.foreach(println)
        val newGroup = ListBuffer[(myVertex, Community)](group._2.reduce((a, b) => if (a._2.members.length > b._2.members.length) a else b))

        //        println(s"Greedy1: after group")
        //        newGroup.foreach(println)
        //        println(s"      ----\n\n")
        newGroup
      }).reduce((a, b) => a ++ b)

      println(s"\tGreedy1: after schedule: ${schedule.length} ")
      //      schedule.sortBy(a=> a._1).foreach(println)

      //      println(s"\n\nSchedule")
      //      schedule.foreach(println)
      val scheduleOptimized: List[(myVertex, Community)] = dynamicScheduler(schedule.toList, Set(), mutable.Map(), 0L)

      //      println(s"\n\nSchedule Optimized")
      //      scheduleOptimized.foreach(println)

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

      println(s"\tGreedy1: optSchedule: ${scheduleOptimized.length}")

      if (scheduleOptimized.length < 1) {

        updated = false
      }
      else {
        updated = true

        commRDD = changeListDelta(graph, commRDD, sc.parallelize(scheduleWithPartingEdges), totEdges)
        commRDD = commRDD.map(c => if (c.members.length < 1) null else c).filter(_ != null).distinct()
        vertexRDD = commRDD.flatMap(c => c.members).map(v => (v.verId, v))
      }

      //      if (scheduleOptimized.length < 3) {
      comPrinted = true
      commRDD.collect().sortBy(c => c.comId).foreach(println)
      //      }
      //      println("\n" + s"x" * 175 + "\n")
      cycle += 1
      if (cycle % 3 == 0) {
        if (!comPrinted)
          commRDD.collect().sortBy(c => c.comId).foreach(println)
      }
      //      if (cycle == 3) {
      //        updated = false
      //    }
      if (cycle == endCycle) {
        updated = false
      }
    } while (updated)


    val endDate = System.currentTimeMillis
    result += s"Execution time: ${(endDate - initDate) / 1000.0}\n\n"
    commRDD
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

}