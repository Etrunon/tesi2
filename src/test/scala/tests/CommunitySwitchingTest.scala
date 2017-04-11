package tests

import myThesis.UtilityFunctions.{loadAndPrepareGraph, readGraph}
import myThesis.{Community, MyMain, myVertex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 11/04/17.
  */
class CommunitySwitchingTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = null
  var edgeFile = "RunData/Input/processed_mini1.csv"
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  override protected def beforeAll(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    val conf = new SparkConf().setAppName("CommTesi2").setMaster("local[1]")
    sc = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  class Fixtures {
    def graphLoaded: Graph[(Long, Long), Long] = {
      readGraph(edgeFile, sc)
    }

    private var uuid: Long = 0L

    private def getUuid: Long = {
      uuid = uuid + 1
      println(s"Current uuid: $uuid")
      uuid
    }

    def createOpFirstRound(vId: Long, degree: Long, com: Long, degreeInCom: Long, edges: (Long, Long)): (myVertex, Community, (Long, Long)) = {
      (new myVertex(vId, vId, degree), new Community(com, ListBuffer[myVertex](new myVertex(com, com, 0)), 0.0), edges)
    }
  }

  val fix = new Fixtures

  "test Add and remove change" should "do what i say" in {
    edgeFile = "RunData/Input/processed_mini1.csv"

    val graph = loadAndPrepareGraph(edgeFile, sc)
    var commRDD = graph.vertices.map(ver => new Community(ver._2.comId, ListBuffer(ver._2), 0.0))

    val data = sc.parallelize(List(
      fix.createOpFirstRound(8, 4, 21, 2, (0, 1)),
      fix.createOpFirstRound(20, 2, 6, 6, (0, 1)),
      fix.createOpFirstRound(1, 3, 3, 3, (0, 1)),
      fix.createOpFirstRound(10, 2, 9, 3, (0, 1)),
      fix.createOpFirstRound(5, 3, 4, 3, (0, 1)),
      fix.createOpFirstRound(25, 2, 26, 6, (0, 1))
    )
    )
    data.foreach(println)

    val switched = MyMain.changeListDelta(graph, commRDD, data, 37)
    //    (Vertex 8 degree 4, member of comm 8,{ Community 21:		 mod 0.0	 members: 21, 	},(0,1))
    //    (Vertex 20 degree 2, member of comm 20,{ Community 6:		 mod 0.0	 members: 6, 	},(0,1))
    //    (Vertex 1 degree 3, member of comm 1,{ Community 3:		 mod 0.0	 members: 3, 	},(0,1))
    //    (Vertex 10 degree 2, member of comm 10,{ Community 9:		 mod 0.0	 members: 9, 	},(0,1))
    //    (Vertex 5 degree 3, member of comm 5,{ Community 4:		 mod 0.0	 members: 4, 	},(0,1))
    //    (Vertex 25 degree 2, member of comm 25,{ Community 26:		 mod 0.0	 members: 26, 	},(0,1))

  }
}
