package com.rishabh.spark
import org.apache.spark.SparkContext
import scala.math.sqrt
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.log4j._

object MovieRecomendation {
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("""R:\study material\hadoop\Spark_Scala\ml-latest-small\movies.csv""").getLines().filter(x=>x.contains("movieId")==false)
     for (line <- lines) {
       var fields = line.split(',')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  def fetchRatings(lines:String)={
    val fields=lines.split(',')
    (fields(0).toInt,(fields(1).toInt,fields(2).toDouble))
  }
  type mapRatings = (Int,Double)
  type UserRatingPair = (Int,(mapRatings,mapRatings))
  def filterDuplicates(ratings:UserRatingPair):Boolean={
    val movieRating1=ratings._2._1
    val movieRating2=ratings._2._2
    val rating1=movieRating1._1
    val rating2=movieRating2._1
    return rating1<rating2
  }

  def makePairs(ratings:UserRatingPair)={
    val movieRating1=ratings._2._1
    val movieRating2=ratings._2._2
    val movie1=movieRating1._1
    val movie2=movieRating2._1
    val rating1=movieRating1._2
    val rating2=movieRating2._2
    
    ((movie1,movie2),(rating1,rating2))
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val path="""R:\study material\hadoop\Spark_Scala\ml-latest-small\movieDemo.csv"""
    val sc=new SparkContext("local[*]","MovieRecomendation")
    val files=sc.textFile(path).filter(x=>x.contains("userId")==false)
    val nameDict=loadMovieNames()
    val ratings=files.map(fetchRatings)
    
    val combineRatings=ratings.join(ratings)
    val filterDuplicate=combineRatings.filter(filterDuplicates)
    /*for(rate <- filterDuplicate){
      println(rate._1+"\t"+rate._2._1._1+"\t"+rate._2._1._2+"\t"+rate._2._2._1+"\t"+rate._2._2._2)
    }*/
    //val filterDuplicate=combineRatings.filter(filterDuplicates)
    val pairs=filterDuplicate.map(makePairs)
    val groupPairs=pairs.groupByKey()
    
    val score=groupPairs.mapValues(computeCosineSimilarity).cache()
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0
      
      val movieID:Int =args(0).toInt
      
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     
      
      val filteredResults = score.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      
    }
      }
    
  }
}