package myThesis

import scala.collection.mutable

/**
  * Created by etrunon on 07/04/17.
  */
object Scheduler {

  /**
    * Function that given a list of possible operations returns a list of compatible operation which should result in the maximized gain
    * It uses dynamic programming with memoization (in the memoization and mapIndex parameters)
    *
    * @param list        list of operation (vertex to change, community toward change it)
    * @param banSet      to be set Set(), on external call set of banned communities
    * @param memoization to be set mutable.Map() on external call
    * @param mapIndex    to be set at 0 on external call
    * @return
    */
  private def dynamicSchedulerInner(list: List[(myVertex, Community)], banSet: Set[Long], memoization: mutable.Map[Long, List[(myVertex, Community)]], mapIndex: Long): List[(myVertex, Community)] = {

    var finale: List[(myVertex, Community)] = List()
    list match {
      case head :: Nil =>
        if (!(banSet.contains(head._1.comId) || banSet.contains(head._2.comId))) {
          finale = List(head)
        }
        else {
          finale = List()
        }
      case head :: tail =>
        // Else if the operation is banned return possible operation without this
        if (banSet.contains(head._1.comId) || banSet.contains(head._2.comId)) {
          finale = dynamicSchedulerInner(tail, banSet, memoization, mapIndex + 1)
        } else {
          //If current operation is not banned
          // Compute the values with current and without
          val withFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex, null) == null) {
            val x = List(head) ::: dynamicSchedulerInner(tail, banSet ++ Set(head._2.comId, head._1.comId), memoization, mapIndex + 1)
            memoization(mapIndex) = x
            x
          } else
            memoization.getOrElse(mapIndex, null)

          val withoutFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex + 1, null) == null) {
            val x = dynamicSchedulerInner(tail, banSet, memoization, mapIndex + 2)
            memoization(mapIndex + 1) = x
            x
          } else
            memoization.getOrElse(mapIndex + 1, null)

          // Whichever is bigger is returned
          //ToDo Test with size and not value
          //          if (withFirst.map(x => x._2.modularity).sum > withoutFirst.map(x => x._2.modularity).sum) {
          if (withFirst.size > withoutFirst.size) {
            finale = withFirst
          }
          else {
            finale = withoutFirst
          }
        }
      case Nil =>
    }
    finale
  }

  /**
    * Wrapper function, to be ale to call easily Scheduler from the outside
    *
    * @param list
    * @return
    */
  def dynamicScheduler(list: List[(myVertex, Community)]): List[(myVertex, Community)] = {
    dynamicSchedulerInner(list, Set(), mutable.Map(), 0L)
  }


  /**
    * Function that given a list of possible operations returns a list of compatible operation which should result in the maximized gain
    * It uses dynamic programming with memoization (in the memoization and mapIndex parameters)
    *
    * @param list        list of operation (vertex to change, community toward change it)
    * @param banSet      to be set Set(), on external call set of banned communities
    * @param memoization to be set mutable.Map() on external call
    * @param mapIndex    to be set at 0 on external call
    * @return
    */
  private def dynamicMergingSchedulerInner(list: List[(Community, Community)], banSet: Set[Long], memoization: mutable.Map[Long, List[(Community, Community)]], mapIndex: Long): List[(Community, Community)] = {

    var finale: List[(Community, Community)] = List()
    list match {
      case head :: Nil =>
        if (!(banSet.contains(head._1.comId) || banSet.contains(head._2.comId))) {
          finale = List(head)
        }
        else {
          finale = List()
        }
      case head :: tail =>
        // Else if the operation is banned return possible operation without this
        if (banSet.contains(head._1.comId) || banSet.contains(head._2.comId)) {
          finale = dynamicMergingSchedulerInner(tail, banSet, memoization, mapIndex + 1)
        } else {
          //If current operation is not banned
          // Compute the values with current and without
          val withFirst: List[(Community, Community)] = if (memoization.getOrElse(mapIndex, null) == null) {
            val x = List(head) ::: dynamicMergingSchedulerInner(tail, banSet ++ Set(head._2.comId, head._1.comId), memoization, mapIndex + 1)
            memoization(mapIndex) = x
            x
          } else
            memoization.getOrElse(mapIndex, null)

          val withoutFirst: List[(Community, Community)] = if (memoization.getOrElse(mapIndex + 1, null) == null) {
            val x = dynamicMergingSchedulerInner(tail, banSet, memoization, mapIndex + 2)
            memoization(mapIndex + 1) = x
            x
          } else
            memoization.getOrElse(mapIndex + 1, null)

          // Whichever is bigger is returned
          //ToDo Test with size and not value
          //          if (withFirst.map(x => x._2.modularity).sum > withoutFirst.map(x => x._2.modularity).sum) {
          if (withFirst.size > withoutFirst.size) {
            finale = withFirst
          }
          else {
            finale = withoutFirst
          }
        }
      case Nil =>
    }
    finale
  }

  /**
    * Wrapper function, to be ale to call easily Scheduler from the outside
    *
    * @param list
    * @return
    */
  def dynamicMergingScheduler(list: List[(Community, Community)]): List[(Community, Community)] = {
    dynamicMergingSchedulerInner(list, Set(), mutable.Map(), 0L)
  }

  def dynamicReachablityScheduler(list: List[(myVertex, Community)], banSet: Set[Long], memoization: mutable.Map[Long, List[(myVertex, Community)]], mapIndex: Long): (List[(myVertex, Community)], Set[Long]) = {

    //    println(" - " * mapIndex.toInt + s"BannList: $banSet")
    var finale: List[(myVertex, Community)] = List()
    list match {
      case head :: Nil =>
        if (!(banSet.contains(head._1.comId) || banSet.contains(head._2.comId))) {
          finale = List(head)
        }
        else {
          finale = List()
        }
      case head :: tail =>
        //        println(" - " * mapIndex.toInt + s"Head: $head")
        //        println(" - " * mapIndex.toInt + s"tail: $tail")
        // Else if the operation is banned return possible operation without this
        if (banSet.contains(head._1.comId) || banSet.contains(head._2.comId)) {
          finale = dynamicReachablityScheduler(tail, banSet, memoization, mapIndex + 1L)._1
        }
        //If current operation is not banned
        else {
          // Compute the values with current and without
          val withFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex, null) == null) {
            val x = List(head) ::: dynamicReachablityScheduler(tail, banSet ++ Set(head._2.comId, head._1.comId), memoization, mapIndex + 1L)._1
            memoization(mapIndex) = x
            x
          } else
            memoization.getOrElse(mapIndex, null)

          val withoutFirst: List[(myVertex, Community)] = if (memoization.getOrElse(mapIndex + 1L, null) == null) {
            val x = dynamicReachablityScheduler(tail, banSet, memoization, mapIndex + 2L)._1
            memoization(mapIndex + 1L) = x
            x
          } else
            memoization.getOrElse(mapIndex + 1L, null)

          // Whichever is bigger is returned
          //          println(" - " * mapIndex.toInt + s"${withFirst.size > withoutFirst.size} ${withFirst.size} > ${withoutFirst.size}")
          if (withFirst.size >= withoutFirst.size) {
            finale = withFirst
          }
          else {
            finale = withoutFirst
          }
        }
      case Nil =>
    }
    //    println(" - " * mapIndex.toInt + s"finale: $finale")
    (finale, banSet)
  }

}
