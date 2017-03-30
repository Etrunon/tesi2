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
class myVertex(deg: Long, cid: Long, vid: Long) extends Serializable with Ordered[myVertex] {
  val degree: Long = deg
  val verId: Long = vid
  var comId: Long = cid
  /** *
    * This value should be normally negative. Meaning that tearing it off its community will result in a loss for it. Otherwise
    * if it ends up being positive it means that putting it into that community was a mistake and it would make the score increase if removed.
    */
  var potentialLoss: Double = 0.0
  var neighList: List[myVertex] = null

  def setNeighbours(vList: List[myVertex]): Unit = {
    neighList = vList
  }

  override def toString: String = s"Vertex $verId degree $degree, member of comm $comId"

  def toStringShort: String = s"$verId, "

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: myVertex => obj.verId == this.verId
      case _ => false
    }
  }

  override def compare(that: myVertex): Int = {
    if (that.verId > verId)
      -1
    else if (that == this)
      0
    else if (that.verId < verId)
      1
    else
      throw new Exception
  }
}
