import java.io.File

import scala.io.Source

object FilesUtils {

  def getListOfDir(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def readFile(fileName: String): String =
  {
    var fileContents = ""
    val source= Source.fromFile(fileName)
    try {
      fileContents = source.getLines.mkString
      fileContents
      //}catch {
      //case e: FileNotFoundException => println("Couldn't find that file.")
      //case e: IOException => println("Got an IOException!")
    }finally {
      source.close() //closing file in JVM
      fileContents
    }
  }
  def readFileTopic(fileName: String): Map[String, String ] =
  {
    //val fileContents = null
    val source= Source.fromFile(fileName)
    try {
      val regex2 = """(?s)(.*)#""".r//ignore comment beginning with #

      val resIteratorArray =
        source.getLines.filter(x=> !(x.take(1)=="#"))//Remove line with only comment
          .map( x => regex2.findFirstIn(x).getOrElse(x).replaceAll("#","") //Get ride of comment
          .replaceAll("\\s", "") //Get ride of white spaces
          .replaceAll("\"", "") //Get ride of quotes
          .split(":"))

      val res=resIteratorArray.map( x => (x(0),x(1))).toMap
      res
    }finally {
      source.close() //closing file in JVM
      //fileContents
    }
  }

  def transformHTTPGetOutputStringToArray(subjectsString: String) : Array[String] ={
    val subjectArray =subjectsString
      .replace("[","")
      .replace("]","")
      .replaceAll("\"", "")
      .split(",")
      .map(_.trim)
    subjectArray
  }


}
