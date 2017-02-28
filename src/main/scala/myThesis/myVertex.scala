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

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: myVertex => obj.verId == this.verId
      case _ => false
    }
  }

}
