package myThesis

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 24/02/17.
  */
/**
  * Object containing a full indipendent community
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

  override def toString: String = s"{ Community $comId:\t members: ${shortMembers()}, mod ${modularity}\t}"

  def shortMembers() = {
    var s: String = ""; for (elem <- members) {
      s += elem.toStringShort
    }; s
  }

  def addToComm(ver: myVertex, newedges: Long, totEdges: Long) = {
    println("ß" * 100)
    val reducingComp = members.map(v => {
      println(s"\nVertici ${v.verId} ${ver.verId}")
      ver.addToComm(v.degree, totEdges)
    }).sum
    members += ver
    modularity += (1.0 / (4.0 * totEdges)) * (newedges + reducingComp)
    //    println(s"New comm modularity $modularity")
    println(s"AddToCom {$ver} to this $this")
    println("ß" * 100)
  }

  def removeFromComm(ver: myVertex, oldEdges: Long, totEdges: Long) = {
    println(s"\tRemoveFromCom {$ver} from this $this")
    members -= ver
    val reducingComp = members.map(v => ver.removeFromComm(v.degree, totEdges)).sum
    modularity += -(1.0 / (4.0 * totEdges)) * (-oldEdges + reducingComp)
    //    println(s"New comm modularity $modularity")
  }

}
