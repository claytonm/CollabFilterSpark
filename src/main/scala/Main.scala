import java.io.{File, BufferedWriter, FileWriter}
import SparkMatrix.Matrix
import org.apache.spark.rdd.RDD
import scala.math._
import scala.io.Source._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {

  // read user-show file
  def readUserShows(fileName: String, sc: SparkContext): RDD[Array[Double]] = {
    sc.textFile(fileName).
      map(l => l.split(" ").map(_.toDouble))
  }

  // read show file
  def readShows(fileName: String): List[(String, Int)] = {
    fromFile(fileName).getLines.toList.zipWithIndex
  }

  // lookup names for all recommended movies
  def getMovieNames(movieRecs: Array[(Int, Double)], shows: List[(String, Int)]): Array[String] = {
    // get movieIDs for recommended movies
    def getMovieIDs(movieRecs: Array[(Int, Double)]): Array[Int] =  movieRecs.map(_._1)

    // lookup movie name from movie ID
    def getMovieName(movieID: Int, shows: List[(String, Int)]): String =
      shows.filter(_._2 == movieID)(0)._1

    getMovieIDs(movieRecs).map(movieID => getMovieName(movieID, shows))
  }

  def getRec(R: Matrix, userIndex: Int, recType: String, shows: List[(String, Int)]): Array[String] = {
    val Rt = R.transpose()
    val P = Matrix.diag(R.rowSum()).functionApply(x => pow(sqrt(x), -1)).getRow(userIndex)
    val Q = Matrix.diag(R.colSum()).functionApply(x => pow(sqrt(x), -1))
    // P * R * Rt * R * Q using only row userIndex
    val recs =
      if (recType == "user")
        (((((P * R).getRow(userIndex)) * Rt).getRow(userIndex)) * R).getRow(userIndex) * Q
      else
        (((((R * Q).getRow(userIndex)) * Rt).getRow(userIndex)) * R).getRow(userIndex) * Q


    val userRec = recs.getRDD.
      filter{case (rowIndex, colIndex, value) => colIndex < 100}.
      map{case (rowIndex, colIndex, value) => (rowIndex, List((colIndex, value)))}.
      reduceByKey((a, b) => a ::: b).
      map{case (rowIndex, values) => (rowIndex, values.sortBy{case (movieID, value) => (-1*value, movieID)})}.
      take(1).map(_._2).flatten

    getMovieNames(userRec, shows)
  }

  def writeRecs(recs: Array[String], top: Int, fileOut: String): Unit = {
    val file = new File(fileOut)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(recs.take(top).mkString("\t"))
    bw.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("Common Friends").
      setMaster("local[4]").
      set("spark.executor.memory", "2g").
      set("spark.driver.memory", "2g")

    val sc = new SparkContext(conf)

    // parameters
    val userShowFile = args(0) // "/Users/clay/education/mmds/data/q1-dataset/q1-dataset/user-shows.txt"
    val showsFile = args(1) // "/Users/clay/education/mmds/data/q1-dataset/q1-dataset/shows.txt"
    val userID = args(2) // 499
    val recMethod = args(3) // "user"
    val fileOut = args(4)  //"recommendstionsForAlex.txt"
    val topMovies = args(5) // 5

    // read user-shows data
    val userShows = readUserShows(userShowFile, sc)

    // read shows
    val shows = readShows(showsFile)

    // convert RDD to Sparse Matrix
    val R = Matrix.rddToSparseMatrix(userShows)

    // get user recs as tab-delimited list of show names
    val userRec = getRec(R, userID, recMethod, shows)

    // write out recommendations
    writeRecs(userRec, topMovies, fileOut)
  }
}
