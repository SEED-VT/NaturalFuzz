package examples.benchmarks

import ai.djl.modality.nlp.DefaultVocabulary
import ai.djl.modality.nlp.bert.BertTokenizer
import ai.djl.modality.nlp.qa.QAInput
import ai.djl.ndarray.NDList
import ai.djl.translate.{Batchifier, Translator, TranslatorContext}

import java.nio.file.Paths
import java.util.List

class BertTranslator extends Translator[QAInput, String] {

  private var tokens: List[String] = null
  private var vocabulary : DefaultVocabulary = null
  private var tokenizer: BertTokenizer = null
  override def prepare(ctx: TranslatorContext): Unit = {
    println("started preparing")
    val path = Paths.get("build/pytorch/bertqa/vocab.txt")
    vocabulary = DefaultVocabulary
      .builder()
      .optMinFrequency(1)
      .addFromTextFile(path)
      .optUnknownToken("[UNK]")
      .build()
    tokenizer = new BertTokenizer()
    println("prepared successfully")
  }

  override def processInput(ctx: TranslatorContext, input: QAInput): NDList = {
    val token = tokenizer.encode(input.getQuestion.toLowerCase, input.getParagraph.toLowerCase)
    // get the encoded tokens that would be used in precessOutput
    tokens = token.getTokens
    val manager = ctx.getNDManager
    // map the tokens(String) to indices(long)
    val indices = tokens.stream.mapToLong(vocabulary.getIndex).toArray
    val attentionMask = token.getAttentionMask.stream.mapToLong((i) => i).toArray
    val tokenType = token.getTokenTypes.stream.mapToLong((i) => i).toArray
    val indicesArray = manager.create(indices)
    val attentionMaskArray = manager.create(attentionMask)
    val tokenTypeArray = manager.create(tokenType)
    // The order matters
    new NDList(indicesArray, attentionMaskArray, tokenTypeArray)
  }

  override def processOutput(ctx: TranslatorContext, list: NDList): String = {
    val startLogits = list.get(0)
    val endLogits = list.get(1)
    val startIdx = startLogits.argMax().getLong().asInstanceOf[Int]
    val endIdx = endLogits.argMax().getLong().asInstanceOf[Int]
    tokens.subList(startIdx, endIdx + 1).toString
  }

  override def getBatchifier: Batchifier = Batchifier.STACK
}

