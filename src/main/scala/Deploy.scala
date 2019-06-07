import com.typesafe.config.ConfigFactory
import java.io.File

object Deploy {
//read yml file key value map
//write a program which call the list schema
  //list version

  def main(args: Array[String] ): Unit = {

    //ConfigFactory.load("application.conf")

    val conf = ConfigFactory.load()
    val ip = conf.getString("schemas.host-ip")
    val port = conf.getInt("schemas.host-port")
    val dir = conf.getString("schemas.folder")

    val result = io.Source.fromURL(s"http://${ip}:${port}").mkString
    print(result)

    //var schemas_states:Map[String,String] = Map()
    var schemas_states:Map[String,Map[Int,String]] = Map()
    //var schemas_states = collection.mutable.Map(String,St)
   // schemas_states = schemas_states +("BP"->"New York")
   // print(schemas_states)

    val listDir = populateSubjects(dir)
    val res= listDir.foreach(x => println(getListOfFiles(x.toString)))
    val res= listDir.foreach(x => println(getListOfFiles(x.toString)))
    println(res)

  }

  //go to folder and list all folder inside folder
  //foreach folder go inside it and get all the file
  //Case class subject , another one version
  case class Version(number: Int, content: String)
  case class Subject( name: String, version: Seq[Version])

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


  def populateSubjects(path: String): List[File] ={
    val res = getListOfDir(path)
    //print(res)
    res
    //Set.empty
  }
}



