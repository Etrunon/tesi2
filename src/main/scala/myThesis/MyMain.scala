package myThesis

import java.io.File
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.util.GraphGenerators

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

    val degrees = tmpGraph.degrees.cache()
    // (Degree, CommId, communityList)
    val graph: Graph[(Long, Long, List[Long]), Long] = tmpGraph.outerJoinVertices(degrees) { (id, _, degOpt) => (degOpt.getOrElse(0).toLong / 2, id, List[Long](id)) }
    //    graph.vertices.collect().foreach(println)
    println("///////////////" * 4)
    // Example 1 Aggregate message
    //    val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)
    //    // Compute the number of older followers and their total age
    //    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
    //      triplet => {
    //        // Map Function
    //        if (triplet.srcAttr > triplet.dstAttr) {
    //          // Send message to destination vertex containing counter and age
    //          triplet.sendToDst(1, triplet.srcAttr)
    //        }
    //      },
    //      // Add counter and age
    //      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    //    )

    // Example 2
    //    // Divide total age by number of older followers to get average age of older followers
    //    val avgAgeOfOlderFollowers: VertexRDD[Double] =
    //      olderFollowers.mapValues((id, value) =>
    //        value match {
    //          case (count, totalAge) => totalAge / count
    //        })
    //     Display the results
    //    avgAgeOfOlderFollowers.collect.foreach(println(_))
    //

    // Example 3 Compute the number of neighbours
    //    val nneb: VertexRDD[Int] = tmpGraph.aggregateMessages[Int](nei => {
    //      nei.sendToDst(1);
    //      nei.sendToSrc(1)
    //    }, (a, b) => a + b)

    //  First test with aggregateMessage
    // It sends a message to every neighbour and just keeps the first one
    // To remove next commit
    val firstRound: VertexRDD[(Long, Long, List[Long])] = graph.aggregateMessages[(Long, Long, List[Long])](neigh => {
      var message = neigh.srcAttr
      neigh.sendToDst(message)
    }, (a, b) => {
      a
    })
    //    firstRound.collect().foreach(println)

    //1 Scrivere funzione che calcola la modularita' di un insieme di nodi
    // 1.1 La funzione considera comunita' x quella in esame e comunita' y tutti glia altri nodi
    // cio'e' sbagliato per ora perche[ non tiene conto delle comunita' multiple.
    //2 Dopodiche' mandare messaggio ai vicini con modularita' e ids
    //3 Chiedere al vicino di quanto si farebbe aumentare o diminuire la mod e farselo rispedire indietro.
    // 3.1 Ogni nodo manda il messaggio. (id, comm, modu)
    // 3.2 Ogni nodo unisce i messaggi ricevuti e quindi sa quanti archi in comune ha con una comm
    //4 Scegliere un vicino
    //5 opt eliminare community circa uguali?
    // tornare a 1

    //Starts an experiment part, to be refactored in helpful methods when ready
    // Community to check atm
    val tmpComm = List[VertexId](1, 2, 3)
    //New graph in which comm vertex are labelled 1 and the others 0
    val newTmpGraph = graph.mapVertices((id, attr) => (attr._1, if (tmpComm.contains(id)) 1L else 0L, attr._3))
    // Pre compute this constant value
    val graphConstant: Float = 1f / (2 * graph.edges.count())
    //It would be 4*totArch, but due to spark doubling archs...
    val totArchs = sc.broadcast(newTmpGraph.edges.count())

    println(s"graphConstant $graphConstant")
    //Rdd contenente il numero di messaggi ricevuti dalla propria uguale comunita' e la lista contentente
    // i gradi dei nodi che ci hanno messaggiato
    //ToDo posso togliere il primo long?!
    val connectedResult: VertexRDD[(Long, List[Long])] = newTmpGraph.aggregateMessages(trip => {
      //If nodes are in the same community
      if (trip.srcAttr._2 == trip.dstAttr._2) {
        trip.sendToDst(1L, List(trip.srcAttr._1))
      }
    }, (a, b) => (a._1 + b._1, a._2 ::: b._2))

    val insideMod = connectedResult.join(newTmpGraph.vertices).map(x => {
      var acc: Float = 0f
      //      println(s"Nodo ${x._1}, grado ${x._2._2._1}, grado dei vicini ${x._2._1._2}, mod $acc")
      for (z <- x._2._1._2) {
        acc = 1 + acc - z.toFloat * x._2._2._1 / totArchs.value
        //        println(s"interno $z tot archi ${totArchi.value}, mod $acc")
      }
      acc
    }).reduce((a, b) => a + b)
    println(s"//////////// Going to print results! ${connectedResult.count()} ////////////")
    println(s"inside community modularity $insideMod, partialFinalMod ${insideMod * graphConstant}")

    //Remove community nodes to keep only out of comm nodes
    // Sends to each neighbour that is in the same community as me my nodeId.
    // then each node know that it will have to compute the value for each other node, not included into the list it has
    //ToDo divide by 2 the result?
    val subgraph1 = newTmpGraph.subgraph(vpred = (id, attr) => attr._2 == 0)
    //    subgraph1.vertices.collect().foreach(println)

    val unconnectedResult: VertexRDD[(Long, List[Long])] = subgraph1.aggregateMessages(trip => {
      if (trip.srcAttr._2 == trip.dstAttr._2) {
        trip.sendToDst((trip.dstAttr._1, List(trip.srcId)))
      }
    }, (a, b) => {
      (a._1, a._2 ::: b._2)
    })

    // The rdd contains
    //  VertexId    ( Degree of node  , List of neighbours  )
    //    4             2                 list(3, 5, 7, 12)
    //
    val test = unconnectedResult.mapValues(neigh => {
      var acc = 0.0
      println(s"Leggo il subgraph? ${subgraph1.vertices}")
      for (vert <- subgraph1.vertices) {
        println(s"Vert c'e'? $vert")
        if (!neigh._2.contains(vert._1)) acc += (vert._2._1.toFloat * neigh._1 / graph.edges.count())
      }
      acc
    })

    test.collect().foreach(println)
  }
}