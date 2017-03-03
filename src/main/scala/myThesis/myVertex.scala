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
  val verId: Long = vid
  var comId: Long = cid
  /** *
    * This value should be normally negative. Meaning that tearing it off its community will result in a loss for it. Otherwise
    * if it ends up being positive it means that putting it into that community was a mistake and it would make the score increase.
    */
  var potentialLoss: Double = 0.0

  override def toString: String = s"Vertex $verId degree $degree, member of comm $comId"

  def toStringShort: String = s"$verId, "

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: myVertex => obj.verId == this.verId
      case _ => false
    }
  }
}
