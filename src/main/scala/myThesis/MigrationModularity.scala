package myThesis

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 04/03/17.
  */
object MigrationModularity {
  def testBundleTestMigration(testBundle: List[List[Long]], graphLoaded: Graph[(Long, Long), Long], sc: SparkContext): ListBuffer[String] = {
    //Timed execution
    val initDate = System.currentTimeMillis
    val degrees = graphLoaded.degrees
    val result = ListBuffer[String]()
    result += "Migration"

    // Generate a graph with the correct formatting
    val graph: Graph[myVertex, Long] = graphLoaded.outerJoinVertices(degrees) { (id, _, degOpt) => new myVertex(id, id, degOpt.getOrElse(0).toLong / 2) }
    // Obtain an RDD containing every community
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, ListBuffer(ver._2), 0.0))
    // Saves edge count co a const
    val totEdges = graph.edges.count() / 2

    // Initialization of delta system. (The graph initially has one vertex for each community, so the first delta should be 0 )
    // Moreover modularity of a single vertex is zero by default
    var oldCom: List[Long] = List()
    // CommId of the vertex will be 1, for testing purpose
    val addingComm = new Community(100L, ListBuffer(), 0.0)
    val removingComm = new Community(200L, ListBuffer(), 0.0)
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

}
