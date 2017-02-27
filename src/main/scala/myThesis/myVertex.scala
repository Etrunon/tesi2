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
  val degree: Long = deg
  var comId: Long = cid
  val verId: Long = vid

  override def toString: String = s"Vertex $verId degree $degree, member of comm $comId"

  def toStringShort: String = s"$verId, "

  def addToComm(otherDeg: Long, totEdges: Long): Double = {
    println(s"Risultato aggiunta vertice $this ${-degree.toDouble * otherDeg.toDouble / (2.0 * totEdges)}")
    -degree.toDouble * otherDeg.toDouble / (2.0 * totEdges)
  }

  def removeFromComm(otherDeg: Long, totEdges: Long): Double = {
    println(s"Risultato sottrazione vertice $this ${-degree.toDouble * otherDeg.toDouble / (2.0 * totEdges)}")
    -degree.toDouble * otherDeg / (2.0 * totEdges)
  }

  def switch(to: Long): Unit = {
    comId = to
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: myVertex => obj.verId == this.verId
      case _ => false
    }
  }

}
