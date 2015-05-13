package org.template.viewedthenboughtproduct

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class CooccurrenceAlgorithmParams(
  n: Int // top co-occurrence
) extends Params

class CooccurrenceModel(
  val topCooccurrences: Map[Int, Array[(Int, Int)]],
  val itemStringIntMap: BiMap[String, Int],
  val items: Map[Int, Item]
) extends Serializable {
  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString(): String = {
    val s = topCooccurrences.mapValues { v => v.mkString(",") }
    s.toString
  }
}

class CooccurrenceAlgorithm(val ap: CooccurrenceAlgorithmParams)
  extends P2LAlgorithm[PreparedData, CooccurrenceModel, Query, PredictedResult] {

  def train(sc: SparkContext, data: PreparedData): CooccurrenceModel = {

    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    val topCooccurrences = trainCooccurrence(
      sequences = data.sequences,
      n = ap.n,
      itemStringIntMap = itemStringIntMap
    )

    // collect Item as Map and convert ID to Int index
    val items: Map[Int, Item] = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }.collectAsMap().toMap

    new CooccurrenceModel(
      topCooccurrences = topCooccurrences,
      itemStringIntMap = itemStringIntMap,
      items = items
    )

  }

  /* given the sequences, find out top n co-occurrence pair for each apriori -> posteriori */
  def trainCooccurrence(
    sequences: RDD[Sequence],
    n: Int,
    itemStringIntMap: BiMap[String, Int]): Map[Int, Array[(Int, Int)]] = {

    val cooccurrences: RDD[((Int, Int), Int)] = sequences
      .flatMap {
        case Sequence(_, apriori, posteriori) =>
          for {
            aprioriElement <- apriori if itemStringIntMap.contains(aprioriElement.item)
            aprioriItemId = itemStringIntMap(aprioriElement.item)
            posterioriElement <- posteriori if itemStringIntMap.contains(posterioriElement.item)
            posterioriItemId = itemStringIntMap(posterioriElement.item)
          } yield ((aprioriItemId, posterioriItemId), 1)
      }
      .reduceByKey{ (a: Int, b: Int) => a + b }

    val topCooccurrences = cooccurrences
      .map{ case (pair, count) =>
        (pair._1, (pair._2, count))
      }
      .groupByKey()
      .map { case (item, itemCounts) =>
        (item, itemCounts.toArray.sortBy(_._2)(Ordering.Int.reverse).take(n))
      }
      .collectAsMap().toMap

    topCooccurrences
  }

  def predict(model: CooccurrenceModel, query: Query): PredictedResult = {

    // convert items to Int index
    val queryList: Set[Int] = query.items
      .flatMap(model.itemStringIntMap.get(_))
      .toSet

    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val blackList: Option[Set[Int]] = query.blackList.map ( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val counts: Array[(Int, Int)] = queryList.toVector
      .flatMap { q =>
        model.topCooccurrences.getOrElse(q, Array())
      }
      .groupBy { case (index, count) => index }
      .map { case (index, indexCounts) => (index, indexCounts.map(_._2).sum) }
      .toArray

    val itemScores = counts
      .filter { case (i, v) =>
        isCandidateItem(
          i = i,
          items = model.items,
          categories = query.categories,
          queryList = queryList,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .sortBy(_._2)(Ordering.Int.reverse)
      .take(query.num)
      .map { case (index, count) =>
        ItemScore(
          item = model.itemIntStringMap(index),
          score = count
        )
      }

    new PredictedResult(itemScores)

  }

  private
  def isCandidateItem(
    i: Int,
    items: Map[Int, Item],
    categories: Option[Set[String]],
    queryList: Set[Int],
    whiteList: Option[Set[Int]],
    blackList: Option[Set[Int]]
  ): Boolean = {
    whiteList.map(_.contains(i)).getOrElse(true) &&
    blackList.map(!_.contains(i)).getOrElse(true) &&
    // discard items in query as well
    (!queryList.contains(i)) &&
    // filter categories
    categories.map { cat =>
      items(i).categories.map { itemCat =>
        // keep this item if has ovelap categories with the query
        !(itemCat.toSet.intersect(cat).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    }.getOrElse(true)
  }

}
