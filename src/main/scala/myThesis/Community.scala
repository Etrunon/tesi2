package myThesis

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 24/02/17.
  */

/**
  * Object containing a fully independent community
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

  /**
    * Util function to compute the reducing component of the modularity formula
    *
    * @param v1
    * @param totEdges
    * @return
    */
  private def membersReducingComp(v1: myVertex, totEdges: Long): Double = {
    members.map(v => {
      -v.degree.toDouble * v1.degree.toDouble / totEdges
      // We do only a b and not b a  so it's not divided by (2 m ) but just ( m )
    }).sum
  }

  /**
    * Function that add the given vertex to the community and updates modularity as result
    *
    * @param ver
    * @param newEdges
    * @param totEdges
    */
  def addToComm(ver: myVertex, newEdges: Long, totEdges: Long): Unit = {
    val reducingComp = membersReducingComp(ver, totEdges)
    members += ver
    ver.comId = comId
    modularity += (1.0 / (4.0 * totEdges)) * (newEdges + reducingComp)
  }

  /**
    * Remove the given vertex from the community and updates modularity score as result
    *
    * @param ver
    * @param oldEdges
    * @param totEdges
    */
  def removeFromComm(ver: myVertex, oldEdges: Long, totEdges: Long): Unit = {
    members -= ver
    if (members.length > 1) {
      val reducingComp = membersReducingComp(ver, totEdges)
      modularity += -(1.0 / (4.0 * totEdges)) * (-oldEdges - reducingComp)
    } else {
      modularity = 0
    }
  }

  /**
    * Function to compute the potential modularity gain from adding provided vertex to the comm
    *
    * @param ver      vertex to be add
    * @param potEdges incoming edges that it would bring
    * @param totEdges total edges of graph, needed as constant
    * @return modularity gain
    */
  def potentialVertexGain(ver: myVertex, potEdges: Long, totEdges: Long): Double = {
    val reducingComp = membersReducingComp(ver, totEdges)
    (1.0 / (4.0 * totEdges)) * (potEdges + reducingComp)
  }

  /**
    * Function to calculate the potential modularity gain from merging provided community in the current one.
    * In potentialEdges only vertex to the new comm are required, as edges that lies inside "com" are already considered in comm.modularity
    *
    * @param com      community to be add
    * @param potEdges map containing potential edges gained from each vertex to the new community
    * @param totEdges total edges of graph, needed as constant
    * @return modularity gain
    */
  def potentialCommunityGain(com: Community, potEdges: Map[Long, Long], totEdges: Long): Double = {
    com.members.map(ver => {
      this.potentialVertexGain(ver, potEdges.getOrElse(ver.verId, 0), totEdges)
    }).sum
  }



}
