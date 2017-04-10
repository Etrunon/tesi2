package tests

import myThesis.{Community, Scheduler, myVertex}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 10/04/17.
  */
class SchedulerTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  class Fixtures {
    private var uuid: Long = 0L

    private def getUuid: Long = {
      uuid = uuid + 1
      println(s"Current uuid: $uuid")
      uuid
    }

    def createOp(vCom: Long, com: Long): (myVertex, Community) = {
      (new myVertex(getUuid, vCom, 0), new Community(com, ListBuffer[myVertex](), 0.0))
    }
  }

  val fix = new Fixtures

  "tests.Scheduler" should "schedule a single operation" in {
    val operation = List(fix.createOp(1, 1))
    val scheduled = Scheduler.dynamicScheduler(operation)

    scheduled should be(operation)
  }

  it should "not schedule two times the same operation" in {
    val opList = List(fix.createOp(1, 2), fix.createOp(1, 2))
    val scheduled = Scheduler.dynamicScheduler(opList)

    opList.size should be(2)
    scheduled.size should be(1)
    scheduled should be(opList.tail)
  }

  it should "remove one conflicting operation in three" in {
    val opList = List(fix.createOp(1, 2), fix.createOp(3, 4), fix.createOp(1, 5))
    val scheduled = Scheduler.dynamicScheduler(opList)

    assert(scheduled.size == 2, scheduled.toString)
    scheduled.size should be(2)
    scheduled should be(opList.reverse.tail.reverse)

  }

  it should "find the best selection of operations" in {
    val dataList = List[(Long, Long)](
      (21, 29),
      (31, 28),
      (27, 31),
      (20, 6),
      (1, 3),
      (10, 9),
      (5, 4),
      (8, 7),
      (8, 21),
      (10, 11),
      (25, 20),
      (31, 30),
      (20, 25),
      (11, 10),
      (31, 27),
      (3, 1),
      (7, 8)
    )
    val opList = dataList.map(t => List(fix.createOp(t._1, t._2))).reduce((a, b) => a ::: b)
    val scheduled = Scheduler.dynamicScheduler(opList)

    scheduled.size should be(7)

    val resultList = List[(Long, Long)](
      (21, 29),
      (31, 28),
      (7, 8),
      (20, 6),
      (1, 3),
      (10, 9),
      (5, 4)
    )
    val resOps = opList.filter(t => resultList.contains((t._1.comId, t._2.comId)))

    scheduled should be(resOps)
  }

  it should "" in {

  }

}



