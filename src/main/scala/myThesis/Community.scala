package myThesis

import scala.collection.mutable.ListBuffer

/**
  * Created by etrunon on 24/02/17.
  */
@SerialVersionUID(124L)
class Community(cid: Long, mod: Double, mlist: ListBuffer[Long]) extends Serializable {
  def comId: Long = cid

  def modularity: Double = mod

  def members: ListBuffer[Long] = mlist

  override def toString: String = s"Community $comId:\t mod $modularity, members: $members"
}
