import java.io.File
import java.net.ConnectException

import FilesUtils._


class Schemas ( val ip: String, val port: Int, val dirSchema: String) {

  //Check the folder topics/mytopic.yml verify that a schema has been registered in schemas/mytopic/mytopic.v1.yml
  def registerSchema( requestSchema: HttpRequestSchema, dirTopics: String) : Map[String, Map[Int, String]] ={
    try {
      val listFilesTopics = getListOfFiles(dirTopics)
      registerSchema(requestSchema, listFilesTopics)
    }catch{
      case e: ConnectException => throw new Exception(e.getMessage)
    }
  }

  def registerSchema( requestSchema: HttpRequestSchema, listFilesTopics: List[File]) : Map[String, Map[Int, String]] ={
    try {
      val listTopics = listFilesTopics.map(x => x.getName.replace(".yml", ""))

      //REPOSITORIES
      // var schemas_states:Map[String,Map[Int,String]] = Map()
      val listDirSchemas = getListOfDir(dirSchema)
      val schemas_states = populateSubjects(listDirSchemas)
      var schemasStatesWanted = schemas_states
      //println("///Schema_states: "+schemas_states)
      var mapSubjectVersionRepo = schemas_states.map(x => (x._1, x._2.map(v => v._1).toList))

      val listSchemaRepoPresent = mapSubjectVersionRepo.map(x => if (x._2.length != 0) {
        x._1
      } else {
        println(s"WARNING: This folder ${'"'}${x._1}${'"'} is empty")
        mapSubjectVersionRepo = mapSubjectVersionRepo.filterKeys(k => k != x._1)
        schemasStatesWanted = schemasStatesWanted.filterKeys(k => k != x._1)
      })

      val listRepoToCompareTopics = listSchemaRepoPresent.map(x => x.toString).filter(x => x != "()")
      val diffExtraSchema = listRepoToCompareTopics.toSet.diff(listTopics.toSet)
      val diffTopicWithNoSchema = listTopics.toSet.diff(listRepoToCompareTopics.toSet)

      //KAFKA
      //list subject Kafka
      val subjectsString = requestSchema.httpGetSubjectList()
      val subjectsArray = transformHTTPGetOutputStringToArray(subjectsString)

      //if subject list is empty in Kafka
      var mapSubjectVersionKafka = Map.empty[String, List[Int]]
      if (subjectsArray(0) != "") {
        val arraySubjectVersion = subjectsArray.map(x => (x, requestSchema.httpGetAllInfoAboutSubject(x)))
        val listSubjectVersionKafka = arraySubjectVersion.map(x => (x._1, x._2.replace("[", "").replace("]", "").split(",").toList)).toMap
        mapSubjectVersionKafka = listSubjectVersionKafka.map(x => (x._1, x._2.map(_.toInt)))
      }

      //Extra in Repo
      val diffExtraRepo: Map[String, List[Int]] = mapSubjectVersionRepo -- mapSubjectVersionKafka.keySet

      //Extra in Kafka
      val diffExtraKafka: Map[String, List[Int]] = mapSubjectVersionKafka -- mapSubjectVersionRepo.keySet
      //println("Extra in Kafka: " + diffExtraKafka)

      //Delete any subject extra in kafka
      diffExtraKafka.foreach(x => requestSchema.httpDeletetSchemaRegistryAndAllVersion(x._1))

      //Post any subject extra in Repo  to Kafka
      schemasStatesWanted.foreach(x => requestSchema.httpPostSchemaRegistryForEachVersion(x._1, x._2))

      schemasStatesWanted
    }catch{
      case e: ConnectException => throw new Exception(e.getMessage)
    }
  }


  def populateSubjects(listDir: List[File]): Map[String,Map[Int,String]] ={//populateVersions
    val schemas_states = listDir.map(x => (x.getName(), populateVersions(x.toPath.toString) ) )
    schemas_states.toMap
  }

  def populateVersions(path: String): Map[Int,String] ={
    val listOfFiles = getListOfFiles(path)
    //Extract version from file name
    val regex = "v(.*).avsc$".r // fetch the version
    val regex2 = "\\d".r // get number from string
    val res =listOfFiles.map( x => (regex2.findFirstIn((regex.findFirstIn(x.toString).get)).get.toInt,readFile(x.toString)))//x.toString))
    //println("This is the list of version: "+ res)
    res.toMap
  }


}
