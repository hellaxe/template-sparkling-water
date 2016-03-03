import io.prediction.controller.{Engine, IEngineFactory}

case class Query(jobTitle: Option[String], category: Option[String]) extends Serializable

case class PredictedResult(category: String) extends Serializable

object VanillaEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("algo" -> classOf[Algorithm]),
      classOf[Serving])
  }
}