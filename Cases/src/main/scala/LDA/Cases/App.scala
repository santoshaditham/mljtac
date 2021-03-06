package LDA.Cases

/**
 * @author ${user.name}
 * This is a project for Juniper Networks - Customer Support team
 * 
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.mllib.clustering.{LDA, KMeans, KMeansModel, GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.classification.NaiveBayes
import kafka.producer.SyncProducerConfig;
import org.apache.spark.streaming.kafka.{ HasOffsetRanges, KafkaUtils }
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import collection.mutable.HashMap
import collection.mutable.Set
import collection.immutable.Map
import java.security.MessageDigest
import scala.collection.immutable.ListMap
import scala.util.control.Breaks.{break, breakable}
import scala.io.Source
/* if we want to include kafka producer*/
//import kafka.serializer.{ DefaultDecoder, StringDecoder }
//import kafka.javaapi.producer.SyncProducer;
//import kafka.javaapi.message.ByteBufferMessageSet;
//import kafka.message.Message;

object App {
  
  /* for unique representation to word/string */
  def md5(s: String) = {
    val base = MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02X".format(_)).mkString
    BigInt(base, 16)
  }
  
  /* each line of text is compared to a problem and solution template */
  def convertToTemplate(input: Array[String], template: Array[String]) : Array[Double] = {
    var output = Array[Double]()
    if(input.size < template.size) return output
    var startIdxs = template.map { x => input.indexWhere(_ == x) }
    var commonSubStrLengths = startIdxs.map { idx =>
      if(idx != -1){
        var inputIdx = idx
        var templateIdx = template.indexOf(input.apply(idx))
        while(inputIdx < input.length && 
            templateIdx < template.length && 
            input.apply(inputIdx) == template.apply(templateIdx)){
          inputIdx += 1
          templateIdx += 1
        }
        (idx, inputIdx-idx)
      }
      else
        (-1,-1)
    }.toMap
    var longestComSubStr = commonSubStrLengths.max
    if(longestComSubStr._1 > -1){
      var front = "<" + input.slice(0, longestComSubStr._1).mkString("-") + ">"
      var middle = input.slice(longestComSubStr._1, longestComSubStr._1 + longestComSubStr._2).mkString(" ")
      var end = "<" + input.slice(longestComSubStr._1 + longestComSubStr._2, input.size).mkString("-") + ">"
      var outputString = front + " "+ middle + " " + end
      output = outputString.split(" ").map(md5(_).toDouble)
    }
    return output
  }
  
  /* dot product between two vectors */
  def dot(x: Array[Double], y: Array[Double]) : Double = {
    var dotProduct = 0.0
    if(x.size == y.size){ dotProduct = (x zip y).map{Function.tupled(_ * _)}.sum }
    return dotProduct
  }
  
  /* squared root of sum of powers */
  def sqrtOfPow(x: Array[Double], y: Array[Double]) : Double = {
    var sqrtValue = 0.0
    if(x.size == y.size){
      val xySquared = (x zip y).map { xy => scala.math.pow(xy._1, 2) + scala.math.pow(xy._2, 2) }.sum
      sqrtValue = scala.math.sqrt(xySquared)
    }
    return sqrtValue 
  }
  
  /* template lines have format = <required> [bunch of keys words] <optional>
   * source lines have format = [bunch of words: technical and non-technical]
   * so we find longest common substring and transform source to look like template
   * and then find cosine similarity
  */
  def cosineSim(sourceLine: String, templateLine: String): Double = {
    var similarity = 0.0
    val templateLineVector = templateLine.split(" ").map(md5(_).toDouble)
    val sourceLineVector = convertToTemplate(sourceLine.split(" "), templateLine.split(" "))
    if(sourceLineVector.length > 0){
      val numerator = dot(sourceLineVector, templateLineVector)
      val denominator = sqrtOfPow(sourceLineVector, templateLineVector)
      similarity = numerator/denominator
    }
    return similarity
  }
  
  /* every line of text is given a score by comparing with pre-defined set of problem and solution templates */
  def lineScore(line: String, t: Int) : Double = {
    var score = 0.0
    val filebufProblems = Source.fromFile("problemsFile.txt")
    val filebufSolutions = Source.fromFile("solutionsFile.txt")
    val typicalProblems = filebufProblems.getLines().toList
    val typicalSolutions = filebufSolutions.getLines().toList
    filebufProblems.close()
    filebufSolutions.close()
    if(t==0){
      val lineProbScores = typicalProblems.map { prob => cosineSim(line, prob) }
      score = lineProbScores.max
    }
    if(t==1){
      val lineSolScores = typicalSolutions.map { sol => cosineSim(line, sol) }
      score = lineSolScores.max
    }
    score
  }
  
  def main(args : Array[String]) {
    // Configs
    val master = "spark://quickstart.cloudera:7077"
    val sparkConf = new SparkConf()
    .set("spark.storage.memoryFraction", ".8")
    .set("spark.executor.memory", "2g")
    .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

    // Contexts    
    val sc = new SparkContext("local[2]","Spark-MLlib",sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)  
    
    // Connect to OracleDB and get case data
    val query = "(select to_char(tnl.objid) as ID, replace(tc.id_number,'-T') as CID, tsite.NAME as CUSTOMER, tc.x_product_series || '--' || tc.x_platform as JHW, tc.x_swrelease_maj as JSW, tc.X_PR_NUMBER ||'--'||tc.X_KB_ID as NUMBERS, tc.X_CASE_CAUSE || '--' || tc.X_CASE_SUBCAUSE as CAUSE, tc.CASE_HISTORY as HISTORY, tu.login_name as CSOWN, tc.title as TITLE, tnl.description as DES, tnl.Internal as Internal from table_notes_log tnl, table_case tc , table_site tsite, table_user tu, table_condition cd where tc.X_CASE_CLASS = 'Support' and tc.x_product_series in ('E-Series', 'EX-Series', 'M-Series', 'MX-Series', 'SRX-Series') and tnl.case_notes2case = tc.objid and tc.id_number like '2014-0202-B%' and tsite.objid = tc.case_reporter2site and tu.objid = tc.case_owner2user and cd.objid = tc.CASE_STATE2CONDITION) t1"
    val serverIP = "jdbc:oracle:thin:savw/savw@//<ip address>/<database>"
    val server = "jdbc:oracle:thin:savw/savw@//<hostname:port>/<database>"
    val driver = "oracle.jdbc.OracleDriver"
    val data = sqlContext.load(source = "jdbc", Map("driver" -> driver, "url" -> serverIP, "dbtable" -> query))  
    println("data row count: " + data.count())
    
    // Regex set
    val ignore = """,;)(}{+#*][|!`="?"""
    val separators = Array[Char](' ', '\n', '\t', '\r')
    val phonePattern = """([0-9]{1,3})[ -.]([0-9]{1,3})[ -.]([0-9]{4,10})""".r
    val datePattern = """([1-9]|0[1-9]|1[012])[- /]([1-9]|0[1-9]|[12][0-9]|3[01])[- /.](\d{2}|\d{4})""".r
    val timePattern = """(0[1-9]|[12][0-9]):(\d{2}):(\d{2})""".r
    val shortTimePattern = """(\d{1}|\d{2}):(\d{2})""".r
    val longTimePattern = """(0[1-9]|[12][0-9]):(\d{2}):(\d{2}).(\d{3})""".r
    val emailPattern = """(\w+)@([\w\.]+)""".r
    val emailPattern1 = """(^[a-zA-Z0-9_-]*$)@([\w\.]+)""".r
    val linkPattern = """(http|https|ftp)://(\w+)""".r
    val numberPattern = """(\d{10})""".r
    val casePattern = """(\d{4})-(\d{4})-(\d{4})""".r
    val tagPattern = """(<(.|\n)+?>)""".r
    val tagPattern1 = """(<^[a-zA-Z0-9_-]*$>)""".r
    val randomPattern = """([.]{3,}|[>]{3,}|[-]{3,})""".r
    val wordPattern1 = """\w*ed\b""".r
    val wordPattern2 = """\w*ly\b""".r
    val wordPattern3 = """\w*ing\b""".r
    
    // get info stored in all help files - dictonary words, stop words, names etc
    val filebufDictionary = Source.fromFile("/usr/share/dict/words")
    val filebufStopWords = Source.fromFile("stopwordsFile.txt")
    val filebufJnprStopWords = Source.fromFile("juniperStopWordsFile.txt")
    val filebufNames = Source.fromFile("namesFile.txt")
    val englishDictionary: scala.collection.Set[String] = filebufDictionary.getLines().toSet
    val stopwords = filebufStopWords.getLines().toList
    val jnprStopwords = filebufJnprStopWords.getLines().toList
    val names = filebufNames.getLines().toList
    filebufDictionary.close()
    filebufStopWords.close()
    filebufJnprStopWords.close()
    filebufNames.close()
    
    /* each column info is stored in a separate RDD, one row per case
     * order of columns: caseid, title, cause, case owners, case history 
     * we infer problem and solution from case history 
     * for later --> we should cache these RDDs for better performance 
     * for now --> since it is all on local machine, no caching */
    val caseIDSets = data.select(data("CID")).map{row =>
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        (thisCaseID -> Set(thisCase))
      }.reduceByKey(_++_)
    val titleWordSets = data.select(data("CID"), data("TITLE")).map{row =>
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        val thisTitleStuff = row.getString(1)
          .toLowerCase()
          .filterNot { ch => ignore.indexOf(ch) >= 0 }
          .split(separators)
          .toSet
        (thisCaseID -> thisTitleStuff)  
      }.reduceByKey(_++_)
    val causeWordSets = data.select(data("CID"), data("CAUSE")).map{row =>
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        var thisCauseStuff = Option(row.getString(1))
            .getOrElse("no cause -- no sub cause")
            .toLowerCase()
            .split("--")
            .toSet
        (thisCaseID -> thisCauseStuff)
      }.reduceByKey(_++_)
    val owners = data.select(data("CID"), data("CSOWN")).map{row =>
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        var thisOwnerStuff = Set(row.getString(1))
        (thisCaseID -> thisOwnerStuff)
      }.reduceByKey(_++_)
    val ownerNames = owners.values.collect()
    val guessedProblemSets = data.select(data("CID"), data("HISTORY"))
      .map{ row => 
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        // try to figure out problem from history using cosine similarity with known templates
        var thisHistory:String = Option(row.getString(1)).getOrElse("no history")
        val thisHistoryLines = thisHistory.toLowerCase().replaceAll("\t", " ").split("\n")
        val thisHistoryProblemLines = thisHistoryLines.slice(0, (thisHistoryLines.length * 0.25).toInt)
        val thisHistoryProblemScores = (thisHistoryProblemLines zip thisHistoryProblemLines.map { lineScore(_,0) }).toMap
        val maxProbScore = thisHistoryProblemScores.find(_._2 == thisHistoryProblemScores.valuesIterator.max).getOrElse(("unable to identify the problem",0.0))
        (thisCaseID -> Set(maxProbScore)) 
      }.reduceByKey(_++_)    
    val guessedSolutionSets = data.select(data("CID"), data("HISTORY"))
      .map{ row => 
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        // try to figure out solution from history using cosine similarity with known templates
        var thisHistory:String = Option(row.getString(1)).getOrElse("no history")
        val thisHistoryLines = thisHistory.toLowerCase().replaceAll("\t", " ").split("\n")
        val thisHistorySolutionLines = thisHistoryLines.slice((thisHistoryLines.length * 0.75).toInt, thisHistoryLines.length)
        val thisHistorySolutionScores = (thisHistorySolutionLines zip thisHistorySolutionLines.map { lineScore(_,1) }).toMap
        val maxSolnScore = thisHistorySolutionScores.find(_._2 == thisHistorySolutionScores.valuesIterator.max).getOrElse(("unable to identify the solution",0.0))
        (thisCaseID -> Set(maxSolnScore)) 
      }.reduceByKey(_++_)
    
    // Get case history, clean it and parse it into an array of words. Do for all cases. Call this as CORPUS  
    val corpus = data.select(data("CID"), data("HISTORY"))
      .map(row => {
        val thisCase = row.getString(0)
        val thisCaseID = md5(thisCase)
        var thisHistory:String = Option(row.getString(1)).getOrElse("no history")
        val splits = thisHistory.toLowerCase()
        .filterNot { ch => ignore.indexOf(ch) >= 0 }
        .split(separators)
        val words = splits.map { w => w
          .stripSuffix(".").stripSuffix(":").stripSuffix(",").stripSuffix("~").stripSuffix("-")
          .stripPrefix(".").stripPrefix(":").stripPrefix(",").stripPrefix("~").stripPrefix("-")}
        val cleanWords = words
          .filterNot { w => stopwords.contains(w) } //stop words
          //.filterNot { w => englishDictionary.contains(w) } //stop words
          .filterNot { w => jnprStopwords.contains(w) } //juniper specific stop words
          .filterNot { w => names.contains(w) } //names
          .filterNot { w => ownerNames.contains(w) } //case owners
          .filterNot { w => w.length()==1 } //single chars
          .filterNot { w => casePattern.unapplySeq(w).isDefined}
          .filterNot { w => emailPattern.unapplySeq(w).isDefined}
          .filterNot { w => emailPattern1.unapplySeq(w).isDefined}
          .filterNot { w => phonePattern.unapplySeq(w).isDefined}
          .filterNot { w => numberPattern.unapplySeq(w).isDefined}
          .filterNot { w => datePattern.unapplySeq(w).isDefined}
          .filterNot { w => timePattern.unapplySeq(w).isDefined}
          .filterNot { w => shortTimePattern.unapplySeq(w).isDefined}
          .filterNot { w => longTimePattern.unapplySeq(w).isDefined}
          .filterNot { w => randomPattern.unapplySeq(w).isDefined}
          .filterNot { w => casePattern.unapplySeq(w).isDefined}
          .filterNot { w => tagPattern.unapplySeq(w).isDefined}
          .filterNot { w => tagPattern1.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern1.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern2.unapplySeq(w).isDefined}
          .filterNot { w => wordPattern3.unapplySeq(w).isDefined}
          .filterNot { w => 
            val z: Seq[Char] = w 
            z match {
              case Seq('h','t','t','p', rest @ _*) => true
              case Seq('f','t','p', rest @ _*) => true
              case Seq('j','u','n','o','s', rest @ _*) => true
              case Seq(_*) => false}}
          .toSet
        (thisCaseID, cleanWords)
      }).reduceByKey(_++_)
    corpus.cache()
    println("Complete: parsed data into corpus -- "+corpus.count())
    
    /* Get columns that contribute to info we already know: customer name, product series, product, platform */
    val custValues  = data.select(data("CID"), data("CUSTOMER"))
      .map{ row => 
        val thisCaseID= md5(row.getString(0))
        val thisCust = Set(row.getString(1))
        (thisCaseID, thisCust)
        }.reduceByKey(_++_)
    val seriesValues  = data.select(data("CID"), data("JHW"))
      .map{ row => 
        val thisCaseID= md5(row.getString(0))
        val thisSeries = Set(row.getString(1).split("--").apply(0))
        (thisCaseID, thisSeries)
        }.reduceByKey(_++_)
    val productValues  = data.select(data("CID"), data("JHW"))
      .map{ row => 
        val thisCaseID= md5(row.getString(0))
        val thisProduct = Set(row.getString(1).split("--").apply(1))
        (thisCaseID, thisProduct)
        }.reduceByKey(_++_)
    val platformValues  = data.select(data("CID"), data("JSW"))
      .map{ row => 
        val thisCaseID= md5(row.getString(0))
        var platform = row.getString(1).toLowerCase()
        if(platform=="please specify") platform = "unknown"
        val thisPlatform = Set(platform)
        (thisCaseID, thisPlatform)
        }.reduceByKey(_++_)
    val givenValues  = data.select(data("CID"), data("CUSTOMER"), data("JHW"), data("JSW"), data("NUMBERS"))
      .map ( row => {
        val caseID= md5(row.getString(0))
        val thisCust:String = Option(row.getString(1)).getOrElse("no customer")
        val cust = (1 / md5(thisCust).toDouble) + 1 //insignificant
        val thisJHW:String = Option(row.getString(2)).getOrElse("no hardware")
        val thisSeries = thisJHW.split("--").apply(0)
        val thisProduct = thisJHW.split("--").apply(1)
        val jhwSeries = md5(thisSeries)%100 //max 100 series
        val jhwProduct = md5(thisProduct)%1000 + 100 //max 1000 products
        val jhw = (jhwProduct + jhwSeries).toDouble 
        val thisJSW:String = Option(row.getString(3)).getOrElse("no software")
        val jsw = (md5(thisJSW)%10000 + 1000).toDouble //max 10,000 platforms
        //val thisOtherNumbers:String = Option(row.getString(4)).getOrElse("no PR -- no KB")
        //val thisPR = thisOtherNumbers.split("--").apply(0)
        //prMap += (thisPR -> (caseID :: (prMap get thisPR getOrElse Nil)))
        //val thisKB = thisOtherNumbers.split("--").apply(1)
        //kbMap += (thisKB -> (caseID :: (kbMap get thisKB getOrElse Nil)))
        (caseID, jsw + jhw + cust)
      }).reduceByKey(_+_)
    givenValues.cache()
    println("Complete: getting other dimension data") 
    
    // Prepare context-aware documents from CORPUS 
    val wordCounts: RDD[(String, Long)] = corpus
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    println("Complete: word count on corpus")
    val fullVocabSize = wordCounts.count()
    val (vocabulary: Map[String, Int], actualNumTokens: Long) = {
        val tmpSortedWC: Array[(String, Long)] = wordCounts.collect().sortBy(-_._2)
        (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
      } 
    val vocabWordArray = new Array[String](vocabulary.size)
    vocabulary.foreach { case (word, i) => vocabWordArray(i) = word }
    println("Complete: create vocabulary from corpus")
  
    val titleWords = titleWordSets.values.flatMap { x => x }.collect()
    val causeWords = causeWordSets.values.flatMap { x => x }.collect()
    val guessedProblems = guessedProblemSets.values.flatMap{ x => x}.collect()
    val guessedSolutions = guessedSolutionSets.values.flatMap{ x => x}.collect()
    val documents = corpus.map { case (id, words) =>
      val docWC = new HashMap[Int, Int]()
      words.foreach { word =>
        if (vocabulary.contains(word)) {
          val wordIndex = vocabulary(word)
          docWC(wordIndex) = docWC.getOrElse(wordIndex, 0) + 1
        }
      }
      val indices = docWC.keys.toArray.sorted
      val values = indices.map{i =>
        var thisWord = vocabWordArray(i)
        var (effectiveCount, longWord, titleWord, causeWord, problemWordScore, solutionWordScore) 
          = (docWC(i),0.0,0.0,0.0,0.0,0.0)
        //RULE: word frequency in doc is not important beyond 9
        if(effectiveCount>9) effectiveCount = 9
        //RULE: word length > 7 means more meaning
        if(thisWord.length()>7) longWord = (md5(thisWord).toDouble % 100) + 10
        //RULE: word belonging to cause is very important
        if(causeWords.contains(thisWord)) causeWord = (md5(thisWord).toDouble % 1000) + 100
        //RULE: word belonging to title is very important
        if(titleWords.contains(thisWord)) titleWord = (md5(thisWord).toDouble % 10000) + 1000
        //RULE: word indicates problem description
        problemWordScore = guessedProblems.find(_._1.contains(thisWord)) match {
          case Some(i) => (i._2 % 10000) + 1000
          case None => 0.0
        }
        //RULE: word indicates solution description
        solutionWordScore = guessedSolutions.find(_._1.contains(thisWord)) match {
          case Some(i) => (i._2 % 10000) + 1000
          case None => 0.0          
        }
        //VALUE for term
        (effectiveCount + longWord + causeWord + titleWord + problemWordScore + solutionWordScore).toDouble
        }
      val numericTokens = Vectors.sparse(vocabulary.size, indices, values)
      (id.toLong, numericTokens)
    }
    documents.cache()
    println("Complete: integer values to documents")
    
    // Cluster the documents into topics using LDA
    val numOfTopics = 2
    val alpha = 50/numOfTopics
    val beta = (200/vocabulary.size.toFloat) + 1
    println(s"alpha is $alpha and beta is $beta")
    val ldaModel = new LDA()
      .setK(numOfTopics)
      .setMaxIterations(30)
      .setDocConcentration(alpha) //high
      .setTopicConcentration(beta) //low
      .run(documents)
    println("Complete: LDA")    
    
    // Interpret results from LDA
    val topics = ldaModel.describeTopics().map { case (termIndices, termWeights) =>
      termWeights.zip(termIndices).map{case(weight, index) => 
        if(index > vocabWordArray.length || index < 0) 
          ("n/a", Double.MinValue)
        else
          (vocabWordArray(index.toInt), weight)}
    }  
    val md5mapVocabulary = vocabulary.map(f => (md5(f._1), f._1))
    
    /* Give technicality for documents using LDA topic classification. 
     * Use only top some% of each topic. That way, words will be relevant */
    val topStuff = topics.map{ t =>
      val cleanStuff = t.toMap
        .filterNot { x => x._2==Double.MinValue }
        .filterNot { x => x._1.length() < 3 } // filter out any small words 
      val topPercent =  (cleanStuff.size * 0.15).toInt //15%
      val cleanStuffSorted = ListMap(cleanStuff.toSeq.sortWith(_._2 > _._2):_*) //descending
      val impStuff = cleanStuffSorted.take(topPercent).keys.toSet
      impStuff
    }.flatten
    val docsAsHighValueTerms = corpus.map { case (id, doc) =>
      val uniqHighValueTerms = doc
        .filter(term => topStuff.contains(term))
        .toSet
      if(!uniqHighValueTerms.isEmpty){
        val md5array = uniqHighValueTerms.map{term =>  md5(term)}.toArray
        (id, md5array)
      }
      else {(id, Array[BigInt](0))}
    } 
    docsAsHighValueTerms.values.cache()
    println("Complete: technical value for docs")
    
    // Cluster the frequent patterns of important words to gain more knowledge from history
    val minSup = 0.25 //very low as items are already unique
    val fpg = new FPGrowth().setMinSupport(minSup) 
    val patterns = fpg.run(docsAsHighValueTerms.values)
    println("Complete: FP growth")

    // Create 2-dimension data [what we know, what we guessed]
    // for future, change this to more dimensions. Else results will be biased
    val fpWords = patterns.freqItemsets.flatMap(_.items.toSet)
    val fpWordsWithEmpty = fpWords.map { word => (word, List[BigInt]()) }.reduceByKey(_ ++ _)
    val wordToDocMap = docsAsHighValueTerms
      .map { case (id, docHigValMD5) => docHigValMD5.map(_ -> id)}
      .flatMap(f => f) 
      .aggregateByKey(List[BigInt]())({case(docIDList, docID) => docIDList.+:(docID) }, _ ++ _)  
    val fpWordsWithDocID = (fpWordsWithEmpty union wordToDocMap).reduceByKey(_ ++ _).filter(f => f._2.length>0)
    val fpDocIDWithWords = fpWordsWithDocID
      .map{ case (word, docs) => docs.map(_ -> word) }
      .flatMap(f => f)
      .aggregateByKey(List[BigInt]())({case(fpWordList, fpWord) => fpWordList.+:(fpWord) }, _ ::: _)
    val computedFPValues = fpDocIDWithWords.map {case (id, fpWordList) => (id, (fpWordList.length * 100).toDouble) }
    val computedValues = docsAsHighValueTerms.map { case (id, docHigValMD5) => (id, docHigValMD5.length.toDouble) }
    val finalComputedValues = (computedValues union computedFPValues).reduceByKey(_+_)
    println("Complete: getting description data into technical dimension")
    
    // Finally, each case is converted into a (x,y) point on a 2D plane
    // As mentioned above, this should be more than 2 dimensions for better results
    val finalValues = givenValues.join(finalComputedValues)
    val points = finalValues.map(row => {
        val point = Vectors.dense(row._2._1, row._2._2) //(x,y)=(given, predicted)
        (row._1, point)
      }
    )
    points.cache()
    println("Complete: getting all dimension data")
    
    // Use k-means to find prior classes
    var numClusters = 20 //good value = number of distinct product series times avg number of problems per product
    var numIterations = 20  
    var priors: List[KMeansModel] = List()
    var errors: List[Double] = List()
    /* need testing: we don't know how many clusters beforehand 
     * so keep clustering until when error is below threshold
     * just add that check condition in while loop below 
     * example: while(errors.isEmpty && errors.last < 5) */
    while(errors.isEmpty){ 
      numClusters+=5
      priors = priors :+ KMeans.train(points.values, numClusters, numIterations)
      errors = errors :+ priors.last.computeCost(points.values)
    }
    val kmeansPredictions = points
      .map {point => (priors.last.predict(point._2) -> List(point._1)) }
      .reduceByKey(_++_)
    val x = points.map(f => (f._1 -> f._2.apply(0))).collect().toMap
    val y = points.map(f => (f._1 -> f._2.apply(1))).collect().toMap
    val xcol1 = caseIDSets.map(f => (f._1 -> f._2.head)).collect().toMap 
    val xcol2 = custValues.map(f => (f._1 -> f._2.head)).collect().toMap
    val xcol3 = seriesValues.map(f => (f._1 -> f._2.head)).collect().toMap
    val xcol4 = productValues.map(f => (f._1 -> f._2.head)).collect().toMap
    val xcol5 = platformValues.map(f => (f._1 -> f._2.head)).collect().toMap
    val ycol1 = causeWordSets.map(f => (f._1 -> f._2.head)).collect().toMap
    val ycol2 = titleWordSets.map(f => (f._1 -> f._2.head)).collect().toMap
    val ycol3 = guessedProblemSets.map(f => (f._1 -> f._2.head._1)).collect().toMap
    val ycol4 = guessedSolutionSets.map(f => (f._1 -> f._2.head._1)).collect().toMap
    val outputClusters = kmeansPredictions
      .filter(cluster => cluster._2.size>1)
      .map{ cluster => 
        val outputVal = cluster._2.map(id => 
          List(cluster._1.toString(),
          x.getOrElse(id, "x not found").toString() ,
          y.getOrElse(id, "y not found").toString() ,
          xcol1.getOrElse(id, "case not found").toString() ,
          xcol2.getOrElse(id, "customer not found").toString() ,
          xcol3.getOrElse(id, "product series not found").toString() ,
          xcol4.getOrElse(id, "product not found").toString() ,
          xcol5.getOrElse(id, "platform not found").toString() ,
          ycol1.getOrElse(id, "cause words not found").toString() ,
          ycol2.getOrElse(id, "title words not found").toString() ,
          ycol3.getOrElse(id, "estimated problem words not found").toString() ,
          ycol4.getOrElse(id, "estimated solution words not found").toString()))
        outputVal
        }
    val outputFolder = "outputFolder"
    outputClusters.map { x => x.map { x => x.mkString("%") } }
    .saveAsTextFile(outputFolder)
    val finalError = errors.last
    println(s"Complete: K means with an error of $finalError")

    
/*    
    // step 2: supervised learning
    // get new case history from wherever thru kafka to spark streams
    Properties props = new Properties();
    props.put(“zk.connect”, “127.0.0.1:2181”);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);
    ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
    producer.send(data);  
    val topic = "test"
    val zk = "localhost:2181"
    val broker = "localhost:9092"
    val sparkConf1 = new SparkConf()
    val sc1 = new SparkContext("local[2]","Spark-Streams-from-Kafka",sparkConf1)
    val ssc = new StreamingContext(sc1, Seconds(10))
    val kafkaConf = Map(
        "metadata.broker.list" -> broker,
        "auto.offset.reset" -> "smallest",
        "zookeeper.connect" -> zk,
        "group.id" -> "spark-streaming-from-kafka",
        "zookeeper.connection.timeout.ms" -> "2500")

    /*Direct Stream from Kafka to Spark*/
    val dstreamFromKafka = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Set(topic)).map(_._2)
    val newCaseDescs = dstreamFromKafka.map { newcase =>
        // do the same stuff we did above for cleaning
        (caseID, cleanWords)
        }
    
    /* Getting Kafka offsets from RDDs */
    dstreamFromKafka.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach(record => println(record)
        }
      }
    
    /* Create naive bayes for matching new case with clusters from step 1 */
    val model = NaiveBayes.train(prior, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = newStuff.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    val wordsFromKafka = dstreamFromKafka.flatMap(_.split(" "))
    val counts = wordsFromKafka.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(10), 2)
    
    ssc.checkpoint("./checkpoints")  
    ssc.start()
    ssc.awaitTermination()
 */  
    }
}
