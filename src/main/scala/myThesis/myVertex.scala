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
class myVertex(deg: Long, cid: Long) extends Serializable {
  def degree = deg

  def comId = cid

  override def toString: String = s"Vertex degree $degree, member of comm $comId"
}
