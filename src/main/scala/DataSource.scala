import grizzled.slf4j.Logger
import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // read all events of EVENT involving ENTITY_TYPE and TARGET_ENTITY_TYPE
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("jobs"),
      eventNames = Some(List("jobs")))(sc)

    val customersRDD: RDD[Job] = eventsRDD.map { event =>
      val customer = try {
        event.event match {
          case "jobs" =>
            Job(Some(event.entityId),
              Some(event.properties.get[String]("jobTitle")),
              Some(event.properties.get[String]("category")))
          case _ => throw new Exception(s"Unexpected event ${event} is read.")
        }
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      customer
    }.cache()

    new TrainingData(customersRDD)
  }
}

@SerialVersionUID(9129684718267757690L) case class Job(
                                                             id: Option[String],
                                                             jobTitle: Option[String],
                                                             category: Option[String]) extends Serializable

class TrainingData(
                    val jobs: RDD[Job]
                    ) extends Serializable {
  override def toString = {
    s"jobs: [${jobs.count()}] (${jobs.take(2).toList}...)"
  }
}
