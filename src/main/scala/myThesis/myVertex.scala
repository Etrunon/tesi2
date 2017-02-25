package myThesis

/**
  * Created by etrunon on 24/02/17.
  */

/**
  * My vertex class. Stores Degree and community membership
  *
  * @param deg degree
  * @param cid community Id
  */
@SerialVersionUID(123L)
class myVertex(deg: Long, cid: Long, vid: Long) extends Serializable {
  val degree = deg
  var comId = cid
  val verId = vid

  override def toString: String = s"Vertex $verId degree $degree, member of comm $comId"

  def toStringShort: String = s"$verId, "

  def addToComm(otherDeg: Long, totEdges: Long): Double = {
    println(s"res ${-degree.toDouble * otherDeg.toDouble / (2.0 * totEdges)}")
    -degree.toDouble * otherDeg.toDouble / (totEdges)
  }

  def removeFromComm(otherDeg: Long, totEdges: Long): Double = {
    degree.toDouble * otherDeg / (totEdges)
  }

  def switch(to: Long): Unit = {
    comId = to
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: myVertex => (obj.verId == this.verId)
      case _ => false
    }
  }

}
