package myThesis

import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

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
    val list = List(
      (0.7350893569, 1L, 3L),
      (0.5344311538, 5L, 3L),
      (0.5283814727, 5L, 6L),
      (0.4748538056, 3L, 6L),
      (0.4536287051, 7L, 3L),
      (0.4519876163, 4L, 7L),
      (0.3703841381, 3L, 2L),
      (0.2295338532, 5L, 7L),
      (0.1766977621, 5L, 4L)

    )

    println(s"\nIniziale\n")
    list.sorted.reverse.foreach(println)
    val sceltaDinamica = schedulerDinamico(list.sorted.reverse, Set[Long](), 0)
    println(s"\nFinale\n")
    sceltaDinamica.foreach(println)

    /*    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
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
        val res4 = strategicCommunityFinder(graphLoaded, sc)

        //    saveResultBulk(res1)
        //    saveResultBulk(res2)
        //    saveResultBulk(res3)
        //    saveResultBulk(res4)

        //    res1.foreach(println)
        //    res2.foreach(println)
        //    res3.foreach(println)
        //    res4.foreach(println)

        // Line to make program stop and being able to view SparkWebUI
        //    readInt()*/
  }

  def schedulerDinamico(list: List[(Double, Long, Long)], bannSet: Set[Long], tab: Int): List[(Double, Long, Long)] = {
    var finale: List[(Double, Long, Long)] = List()
    list match {
      case head :: Nil => {
        if (!(bannSet.contains(head._2) || bannSet.contains(head._3))) {
          println(" - " * tab + s"Last one $head is not banned in $bannSet")
          finale = List(head)
        }
        else {
          println(" - " * tab + s"Last one $head is banned $bannSet")
          finale = List()
        }
      }

      case head :: tail => {

        // Else if the operation is banned return possible operation without this
        if (bannSet.contains(head._2) || bannSet.contains(head._3)) {
          println(" - " * tab + s"Head $head is banned in $bannSet")
          finale = schedulerDinamico(tail, bannSet, tab + 1)
        }
        //If current operation is not banned
        else {
          println(" - " * tab + s"Head $head is not banned in $bannSet")
          // Compute the values with current and without
          val withFirst: List[(Double, Long, Long)] = List(head) ::: schedulerDinamico(tail, bannSet ++ Set(head._2, head._3), tab + 1)
          val withoutFirst: List[(Double, Long, Long)] = schedulerDinamico(tail, bannSet, tab + 1)

          println(" - " * tab + s"WithFirst\t\t ${withFirst.map(x => x._1).sum}")
          println(" - " * tab + s"WithoutFirst\t\t ${withoutFirst.map(x => x._1).sum}")
          println(" - " * tab + s"WithFirst\t\t ${withFirst}")
          println(" - " * tab + s"WithFirst\t\t ${withoutFirst}")

          // Whichever is bigger is returned
          if (withFirst.map(x => x._1).sum > withoutFirst.map(x => x._1).sum) {
            println(" - " * tab + s"I take $head")
            finale = withFirst
          }
          else {
            println(" - " * tab + s"I don't take $head")
            finale = withoutFirst
          }
          println(" - " * tab + s"")
          println(" - " * tab + s"")

        }
      }
      case _ => {
        println(" - " * tab + s"Non dovrei mai finire qui")
        finale = List()
      }
    }
    finale
  }


  def strategicCommunityFinder(graphLoaded: Graph[(Long, Long), Long], sc: SparkContext): ListBuffer[String] = {
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

    var vertexRDD: RDD[(Long, myVertex)] = graph.vertices.map(v => (v._1, v._2))

    //    println(s"\n\nComunita' divise per membri")
    //    commRDD.map(c => c.members).collect().foreach(println)
    println(s"\n\nVertici")
    vertexRDD.collect().foreach(println)

    var updated = false

    do {
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
        val boh = group._2.filterNot(g => g._1.comId == currComm).groupBy(dver => dver._1.verId).map(srcGroup => {
          val countedGroup = srcGroup._2.map(g => (g._1, 1L)).reduce((a, b) => (a._1, a._2 + b._2))
          countedGroup
        })
        (currComm, boh)
      })


      //      println(s"CorrectrNeighCounts")
      //      correctNeighCounts.collect().foreach(println)

      /*
            // Takes a graph triplets rdd and return a map of communities with their frontier neighbours and how many times they link with each neighbour
            val OLDcommNeighCounts = graph.triplets.groupBy(tri => {
              tri.dstAttr.comId
            }).map(groupedComm => {
              (groupedComm._1, groupedComm._2.filterNot(f => f.srcAttr.comId == groupedComm._1).groupBy(tri => tri.srcAttr.verId).map(groupedTriplets => {
                groupedTriplets._2.map(tri => {
                  (tri.srcAttr, 1L)
                }).reduce((a, b) => (a._1, a._2 + b._2))
              }))
            })
      */
      //      println(s"\n\n\nCommNeigh")
      //      println(s"${commNeighCounts.collect().foreach(println)}")

      val indexedComm = commRDD.map(co => (co.comId, co))

      val finalImprovement = commNeighCounts.join(indexedComm).map(union => {
        union._2._1.map(ver => {
          (union._2._2.scoreDifference(ver._1, ver._2, totEdges), List[(myVertex, Community)]((ver._1, union._2._2)))
        })
      }).reduce((a, b) => {
        (a.keySet ++ b.keySet).map(i => (i, a.getOrElse(i, List[(myVertex, Community)]()) ::: b.getOrElse(i, List[(myVertex, Community)]()))).toMap
      })

      //    println(s"\n\n\nFinalImprovement \n ${finalImprovement}")

      val bannList = mutable.ListBuffer[Long]()
      val schedule = ListBuffer[(myVertex, Community)]()
      finalImprovement.keySet.toList.sorted.reverse.foreach(k => {

        val curr = finalImprovement.get(k)
        val result = mutable.ListBuffer[(myVertex, Community)]()
        curr.map(change => {
          change.foreach(tuple => {
            // Add (node-> comm) to the schedule. Also symmetric operations


            if (!bannList.contains(tuple._1.comId) && !bannList.contains(tuple._2.comId)) {
              result += tuple
              bannList += (tuple._1.comId, tuple._2.comId)
            }
          })
          schedule.++=(result)
        })
      })

      //      println(s"\n\nScheduler")
      //      schedule.foreach(v => println(s"vertex (${v._1.verId}, ${v._1.comId}) to comm ${v._2.comId}"))

      val scheduleWithPartingEdges = schedule.map(sc => {
        graph.triplets.map(tri => {
          if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._1.comId)
            (sc._1, sc._2, (1L, 0L))
          else if (tri.srcAttr.verId == sc._1.verId && tri.dstAttr.comId == sc._2.comId)
            (sc._1, sc._2, (0L, 1L))
          else
            (sc._1, sc._2, (0L, 0L))
        }).reduce((a, b) => (a._1, a._2, (a._3._1 + b._3._1, a._3._2 + b._3._2)))
      })

      //            println(s"\n\nSchedule following:")
      //            schedule.foreach(println)

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

      println("\n" + s"x" * 175 + "\n")


    } while (updated == true)

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

  def changeListDelta(graph: Graph[myVertex, VertexId], commRDD: RDD[Community], changeList: RDD[(myVertex, Community, (Long, Long))], totEdges: Long): RDD[Community] = {

    //    println(s"in funzione")
    //    println(s"Changelist")
    // Select the community and the node to which add and the amount of new edges
    val addChangeCom = changeList.map(cl => (cl._2.comId, (cl._1, cl._3._2)))
    // Select the community from which the node has to be removed and the amount of edges that it brings out
    val removeChangeCom = changeList.map(cl => (cl._1.comId, (cl._1, cl._3._1)))

    // Select the communities and expose the index
    val exposedComm = commRDD.map(c => (c.comId, c))
    //    exposedComm.collect().foreach(println)

    println(s"\n\nAddChangeCom")
    addChangeCom.map(ac => {
      s"Comm ${ac._1} add ${ac._2._2}"
    }).collect().foreach(println)
    println(s"\n\nRemoveChangeComm")
    removeChangeCom.map(rc => {
      s"Comm ${rc._1} remove ${rc._2._2}"
    }).collect().foreach(println)
    //    println(s"\n\nExposedComm")
    //    exposedComm.collect().foreach(println)

    //    println(s"Join?")
    /*
        val joined: RDD[(Community, (myVertex, Long), (myVertex, Long))] = exposedComm.fullOuterJoin(addChangeCom).map(j => {
          val index: Long = j._1
          val community: Community = j._2._1.orNull
          val add: (myVertex, Long) = j._2._2.orNull
          (index, (community, add))
        }).fullOuterJoin(removeChangeCom).map( j => {
          val community: Community = j._2._1.orNull._1
          val add: (myVertex, Long) = j._2._1.orNull._2
          val remove: (myVertex, Long) = j._2._2.orNull
          (community, add, remove)
        })
    */

    /*
        //    println(s"\n\nPrimaJoin")
        exposedComm.fullOuterJoin(addChangeCom).map(j => {
          val index: Long = j._1
          val community: Community = j._2._1.orNull
          val add: (myVertex, Long) = j._2._2.orNull
          (index, (community, add))
        }).collect().foreach(println)
    */

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

      if (comm == null)
        null
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

}