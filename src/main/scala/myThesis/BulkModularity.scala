package myThesis

import myThesis.UtilityFunctions.modularity
import org.apache.spark.graphx.Graph

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 04/03/17.
  */
object BulkModularity {

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

}
