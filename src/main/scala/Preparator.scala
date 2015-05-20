package org.template.viewedthenboughtproduct

import io.prediction.controller.{Params, PPreparator}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

case class PreparatorParams(sessionTimeout: Long) extends Params

class Preparator(pp: PreparatorParams)
  extends PPreparator[TrainingData, PreparedData] {

  @tailrec
  private def aprioriPhase(sortedSiteEvents: List[SiteEvent], prev_t: Long, acc: List[SequenceElement]): (List[SiteEvent], List[SequenceElement], Long) = {
    sortedSiteEvents match {
      case SiteEvent(SiteEventType.View, _, item, t) :: tail if t - prev_t < pp.sessionTimeout =>
        aprioriPhase(tail, t, SequenceElement(item, t) :: acc)
      case _ =>
        (sortedSiteEvents, acc, prev_t)
    }
  }

  @tailrec
  private def posterioriPhase(sortedSiteEvents: List[SiteEvent], acc: List[SequenceElement]): (List[SiteEvent], List[SequenceElement]) = {
    sortedSiteEvents match {
      case SiteEvent(SiteEventType.Buy, _, item, t) :: tail =>
        posterioriPhase(tail, SequenceElement(item, t) :: acc)
      case _ =>
        (sortedSiteEvents, acc)
    }
  }

  @tailrec
  private def sequencer(user: String, sortedSiteEvents: List[SiteEvent], apriori: List[SequenceElement], prev_t: Long, result: List[Sequence]): List[Sequence] = {
    if (sortedSiteEvents.isEmpty) {
      result
    } else {
      sortedSiteEvents.head match {
        case (SiteEvent(SiteEventType.View, _, item, t)) =>
          val (remainingSiteEvents, newApriori, last_t) = aprioriPhase(sortedSiteEvents, t, List.empty)
          sequencer(user, remainingSiteEvents, newApriori, last_t, result)
        case (SiteEvent(SiteEventType.Buy, _, item, t)) if t - prev_t < pp.sessionTimeout =>
          //Item is bought in the same session
          val (remainingSiteEvents, posteriori) = posterioriPhase(sortedSiteEvents, List.empty)
          sequencer(user, remainingSiteEvents, List.empty, 0, Sequence(user, apriori, posteriori) :: result)
        case (SiteEvent(SiteEventType.Buy, _, item, t)) =>
          //Item is bought in another session
          val (remainingSiteEvents, posteriori) = posterioriPhase(sortedSiteEvents, List.empty)
          sequencer(user, remainingSiteEvents, List.empty, 0, Sequence(user, List.empty, posteriori) :: result)
        case _ =>
          sequencer(user, sortedSiteEvents.tail, apriori, prev_t, result)
      }
    }
  }

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    //prepare session sequences to infer views and buys in sequence per user per session
    val sequences = trainingData.siteEvents
      .groupBy(_.user)
      .flatMap {
        case (user, siteEvents) =>
          //Sorting siteEvents of a single user shouldn't be a problem for a single node
          val sortedEvents = siteEvents.toList.sortBy(_.t)
          sequencer(user, sortedEvents, List.empty, sortedEvents.head.t, List.empty)
      }

    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      sequences = sequences)
  }
}

case class SequenceElement(item: String, t: Long)
case class Sequence(user: String, apriori: List[SequenceElement], posteriori: List[SequenceElement])

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val sequences: RDD[Sequence]
) extends Serializable
