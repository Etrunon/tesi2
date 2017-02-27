package myThesis

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 24/02/17.
  */

/**
  * Object containing a full independent community
  *
  * @param cid   Comm id
  * @param mod   Modularity value
  * @param mlist MemberList
  */
@SerialVersionUID(124L)
class Community(cid: Long, mod: Double, mlist: ListBuffer[myVertex]) extends Serializable {
  val comId: Long = cid
  var modularity: Double = mod
  val members: ListBuffer[myVertex] = mlist

  override def toString: String = s"{ Community $comId:\t members: ${shortMembers()}, mod $modularity\t}"

  def shortMembers(): String = {
    var s: String = ""
    for (elem <- members) {
      s += elem.toStringShort
    }
    s
  }

  def membersReducingComp(v1: myVertex, totEdges: Long): Double = {
    members.map(v => {
      println(s"\nVertici ${v.verId} ${v1.verId}")
      -v.degree.toDouble * v1.degree.toDouble / totEdges
      // We do only a b and not b a  so it's not divided by (2 m ) but just ( m )
    }).sum
  }

  def addToComm(ver: myVertex, newEdges: Long, totEdges: Long): Unit = {
    println("ß" * 100)
    println(s"BEFORE AddToCom {$ver} to this $this")

    val reducingComp = membersReducingComp(ver, totEdges)

    members += ver

    println(s" (1.0 / (4.0 * totEdges)) * (newedges + reducingComp)" +
      s"\n(1.0 / (4.0 * $totEdges)) * ($newEdges + $reducingComp)" +
      s"\n(${1.0 / (4.0 * totEdges)}) * (${newEdges + reducingComp})" +
      s"\n${(1.0 / (4.0 * totEdges)) * (newEdges + reducingComp)}" +
      s"\nFine AddCom\n\n")

    modularity += (1.0 / (4.0 * totEdges)) * (newEdges + reducingComp)
    //    println(s"New comm modularity $modularity")
    println(s"AFTER AddToCom {$ver} to this $this")
    println("ß" * 100)
  }

  def removeFromComm(ver: myVertex, oldEdges: Long, totEdges: Long): Unit = {
    println("←" * 100)
    println(s"\tBEFORE RemoveFromCom {$ver} from this $this")


    members -= ver

    if (members.length > 1) {

      val reducingComp = membersReducingComp(ver, totEdges)

      modularity += -(1.0 / (4.0 * totEdges)) * (-oldEdges - reducingComp)

      //    println(s"New comm modularity $modularity")
      println(s" (1.0 / (4.0 * totEdges)) * (newedges + reducingComp)" +
        s"\n(1.0 / (4.0 * $totEdges)) * ($oldEdges + $reducingComp)" +
        s"\n(${1.0 / (4.0 * totEdges)}) * (${oldEdges + reducingComp})" +
        s"\n${(1.0 / (4.0 * totEdges)) * (oldEdges + reducingComp)}" +
        s"\nFine RemoveFromCom\n\n")

    } else {
      modularity = 0
    }


    println(s"\tAFTER RemoveFromCom {$ver} from this $this")
    println("←" * 100)

  }

}