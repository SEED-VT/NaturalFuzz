package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

import java.io._
import java.nio.file._
import ai.djl.training.util._
import ai.djl.repository.zoo._
import ai.djl.modality.nlp._
import ai.djl.modality.nlp.qa._
import ai.djl.modality.nlp.bert._
import org.apache.log4j.BasicConfigurator


object NLPCaseStudy extends Serializable {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    //    val conf = new SparkConf()
    //    conf.setMaster("local[*]")
    //    conf.setAppName("NLP Case Study")
    //    val sc = new SparkContext(conf)
    //    val dataset = "seeds/full_data/okcupid/okcupid_profiles.csv"
    //    val okcupid = sc.textFile(dataset)

    val question = "When did BBC Japan start broadcasting?"
    val resourceDocument = "BBC Japan was a general entertainment Channel.\n" + "Which operated between December 2004 and April 2006.\n" + "It ceased operations after its Japanese distributor folded."
    val input = new QAInput(question, resourceDocument)

    val tokenizer = new BertTokenizer()
    val tokenQ = tokenizer.tokenize(question.toLowerCase)
    val tokenA = tokenizer.tokenize(resourceDocument.toLowerCase)

    println("Question Token: " + tokenQ)
    println("Answer Token: " + tokenA)

    val tokens = tokenizer.encode(question.toLowerCase, resourceDocument.toLowerCase)
    println("Encoded tokens: " + tokens.getTokens)
    println("Encoded token type: " + tokens.getTokenTypes)
    println("Valid length: " + tokens.getValidLength)
    //    okcupid.collect().foreach(println)

    //    DownloadUtils.download(
    //      "https://djl-ai.s3.amazonaws.com/mlrepo/model/nlp/question_answer/ai/djl/pytorch/bertqa/0.0.1/bert-base-uncased-vocab.txt.gz",
    //      "build/pytorch/bertqa/vocab.txt",
    //      new ProgressBar())

    val path = Paths.get("build/pytorch/bertqa/vocab.txt")
    val vocabulary = DefaultVocabulary
      .builder
      .optMinFrequency(1)
      .addFromTextFile(path).
    optUnknownToken("[UNK]")
      .build

    val index = vocabulary.getIndex("car")
    val token = vocabulary.getToken(2481)
    println("The index of the car is " + index)
    println("The token of the index 2481 is " + token)

    //    DownloadUtils.download(
    //      "https://djl-ai.s3.amazonaws.com/mlrepo/model/nlp/question_answer/ai/djl/pytorch/bertqa/0.0.1/trace_bertqa.pt.gz",
    //      "build/pytorch/bertqa/bertqa.pt",
    //      new ProgressBar())
    val translator = new BertTranslator()

    val criteria = Criteria
      .builder()
      .setTypes(classOf[QAInput], classOf[String])
      .optTranslator(translator)
      .optModelPath(Paths.get("build/pytorch/bertqa/"))
      .optProgress(new ProgressBar())
      .build()

    println("done")
    val model = criteria.loadModel()

    var predictResult = ""
//    val input = new QAInput(question, resourceDocument)

    // Create a Predictor and use it to predict the output
    try {
      val predictor = model.newPredictor(translator)
      try predictResult = predictor.predict(input)
      finally if (predictor != null) predictor.close()
    }

    println(question)
    println(predictResult)

  }

}