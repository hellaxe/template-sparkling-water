import grizzled.slf4j.Logger
import hex.deeplearning.{DeepLearning, DeepLearningModel, DeepLearningParameters}
import io.prediction.controller.{IPersistentModel, P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.h2o._


// import hex.Model
// import hex.Model.Output
// import org.apache.spark.h2o._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
// import org.apache.spark.rdd.RDD
// import org.apache.spark.sql.SQLContext
// import org.apache.spark.{SparkContext, mllib}
// import water.app.{GBMSupport, ModelMetricsSupport, SparkContextSupport, SparklingWaterApp}

case class AlgorithmParams(epochs: Int) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  val STOP_WORDS = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

  val EMPTY_PREDICTION = ("NA", Array[Double]())



  def train(sc: SparkContext, data: PreparedData): Model = {
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    val result = new H2OFrame(data.jobs)

    val dlParams: DeepLearningParameters = new DeepLearningParameters()


    dlParams._train = result
    dlParams._response_column = 'category
    dlParams._epochs = ap.epochs


    //
    // -- TODO: more ways to filter & combine data
    //
    // val customerTable: H2OFrame = new H2OFrame(data.customers)

    //
    // -- Run DeepLearning
    //

    // val dlParams = new DeepLearningParameters()
    // dlParams._train = customerTable
    // dlParams._response_column = 'churn
    // dlParams._epochs = ap.epochs

    val dl: DeepLearning = new DeepLearning(dlParams)
    val dlModel: DeepLearningModel = dl.trainModel.get
    new Model(dlModel = dlModel, sc = sc, h2oContext = h2oContext)
  }

  def predict(model: Model, query: Query): PredictedResult = {

    //
    // -- Make prediction
    //
    import model.h2oContext._

    val inputQuery = Seq(Input(query.jobTitle))
    // Job(None, Some(query.jobTitle), None)

    val jobsQueryDataFrame = new H2OFrame(model.sc.parallelize(inputQuery))
    val predictionH2OFrame = model.dlModel.score(jobsQueryDataFrame)('predict)
    val predictionsFromModel =
      toRDD[StringHolder](predictionH2OFrame).
      map ( _.result.getOrElse("None") ).collect
    
    new PredictedResult(category = predictionsFromModel(0))
  }

  def wordToVector(word: String, model: Word2VecModel): Vector = {
    try {
      return model.transform(word)
    } catch {
      case e: Exception => return Vectors.zeros(100)
    }
  }

  // def createH2OFrame(h2oContext: H2OContext, data: RDD[Job]): (H2OFrame, Word2VecModel) = {
  //   // Load data from specified file
  //   val dataRdd = data

  //   // Compute rare words
  //   val tokenizedRdd = dataRdd.map(d => (d.category.getOrElse("(not given)"), tokenize(d.jobTitle.getOrElse("(not given)"), STOP_WORDS)))
  //                       .filter(s => s._2.length > 0)
  //   // Compute rare words
  //   val rareWords = computeRareWords(tokenizedRdd.map(r => r._2))
  //   // Filter all rare words
  //   val filteredTokenizedRdd = tokenizedRdd.map(row => {
  //     val tokens = row._2.filterNot(token => rareWords.contains(token))
  //     (row._1, tokens)
  //   }).filter(row => row._2.length > 0)

  //   // Extract words only to create Word2Vec model
  //   val words = filteredTokenizedRdd.map(v => v._2.toSeq)

  //   // Create a Word2Vec model
  //   val w2vModel = new Word2Vec().fit(words)

  //   // Sanity Check
  //   w2vModel.findSynonyms("teacher", 5).foreach(println)
  //   // Create vectors

  //   val finalRdd = filteredTokenizedRdd.map(row => {
  //     val label = row._1
  //     val tokens = row._2
  //     // Compute vector for given list of word tokens, unknown words are ignored
  //     val vec = wordsToVector(tokens, w2vModel)
  //     JobOffer(label, vec)
  //   })

  //   // Transform RDD into DF and then to HF
  //   import h2oContext._
  //   import sqlContext.implicits._
  //   val h2oFrame: H2OFrame = finalRdd.toDF
  //   h2oFrame.replace(h2oFrame.find("category"), h2oFrame.vec("category").toCategoricalVec).remove()

  //   (h2oFrame, w2vModel)
  // }

  // def computeRareWords(dataRdd : RDD[Array[String]]): Set[String] = {
  //   // Compute frequencies of words
  //   val wordCounts = dataRdd.flatMap(s => s).map(w => (w,1)).reduceByKey(_ + _)

  //   // Collect rare words
  //   val rareWords = wordCounts.filter { case (k, v) => v < 2 }
  //     .map {case (k, v) => k }
  //     .collect
  //     .toSet
  //   rareWords
  // }

  // private def tokenize(line: String, stopWords: Set[String]) = {
  //   //get rid of nonWords such as punctuation as opposed to splitting by just " "
  //   line.split("""\W+""")
  //     // Unify
  //     .map(_.toLowerCase)

  //     //remove mix of words+numbers
  //     .filter(word => ! (word exists Character.isDigit) )

  //     //remove stopwords defined above (you can add to this list if you want)
  //     .filterNot(word => stopWords.contains(word))

  //     //leave only words greater than 1 characters.
  //     //this deletes A LOT of words but useful to reduce our feature-set
  //     .filter(word => word.size >= 2)
  // }



  // private def wordsToVector(words: Array[String], model: Word2VecModel): Vector = {
  //   val vec = Vectors.dense(
  //     divArray(
  //       words.map(word => wordToVector(word, model).toArray).reduceLeft(sumArray),
  //       words.length))
  //   vec
  // }
  // private def wordToVector(word: String, model: Word2VecModel): Vector = {
  //   try {
  //     return model.transform(word)
  //   } catch {
  //     case e: Exception => return Vectors.zeros(100)
  //   }
  // }

}



// case class JobOffer(category: String, fv: mllib.linalg.Vector)
case class Input(jobTitle: Option[String])

class Model(val dlModel: DeepLearningModel, val sc: SparkContext,
            val h2oContext: H2OContext)
  extends IPersistentModel[AlgorithmParams] {

  // Sparkling water models are not deserialization-friendly
  def save(id: String, params: AlgorithmParams, sc: SparkContext): Boolean = {
    false
  }
}
