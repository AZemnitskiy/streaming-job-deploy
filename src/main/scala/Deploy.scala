import java.io.File

import com.persist.JsonOps._
import com.typesafe.config.ConfigFactory
import scalaj.http.{Http, HttpResponse}

import scala.io.Source



object Deploy {

  val usage = """
    Usage: deploy application.conf
  """

  def main(args: Array[String] ): Unit = {
    try {
      val myConfigFile = new File(args(0))
      val conf = ConfigFactory.parseFile(myConfigFile)

      val path = conf.getString("target.folder")
      val ip = conf.getString("schemas.host-ip")
      val port = conf.getInt("schemas.host-port")

      val pathFolder = new File(path)
      val dirSchema1 = new File(conf.getString("schemas.folder"))
      val dirSchema = (pathFolder.getPath + File.separator + dirSchema1.getPath).replaceAll("\\\\", "/")

      val ipTopics = conf.getString("topics.host-ip")
      val portTopicsKafkaManager = conf.getInt("topics.host-port-kafka-manager")
      val portTopics = conf.getInt("topics.host-port")
      val dirTopic1 = new File(conf.getString("topics.folder"))
      val dirTopics = (pathFolder.getPath + File.separator + dirTopic1.getPath).replaceAll("\\\\", "/")
      val clusterName = conf.getString("topics.cluster")
      val zkHosts = conf.getString("topics.zkHosts")
      val kafkaVersion = conf.getString("topics.kafkaVersion")

      val requestSchema= new HttpRequestSchema(ip,port)
      val requestTopic= new HttpRequestTopic(ipTopics,portTopicsKafkaManager, portTopics)

      val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

      //##############Register Schema #################
      val schemaRegistered = registerSchema(requestSchema,ip, port, dirSchema, listFilesTopicsFromRepo)

      //##############Publish Topics #################
      //Once Schema is registered, push topics on Kafka:
      //-if topics already exist update it
      //-if new topics, publish it
      createOrUpdateTopics(requestSchema , requestTopic, ip, port, ipTopics, portTopicsKafkaManager, zkHosts: String, kafkaVersion: String, portTopics, dirSchema, dirTopics, clusterName, listFilesTopicsFromRepo, schemaRegistered)
    }catch{
      case e: Exception => {println("exception caught: " + e.getMessage)
                            System.exit(-1)
                            }

    }
  }

  case class TopicsConf( ip: String, portTopicsKafkaManager: Int, port: Int, folder: String, cluster: String, zkHosts: String, kafkaVersion: String)
  case class SchemasConf( ip: String, port: Int, dirSchema: String)


  def createOrUpdateTopics(requestSchema: HttpRequestSchema, requestTopic: HttpRequestTopic, ip:String, port: Int, ipTopics: String, portTopicsKafkaManager: Int, zkHosts: String, kafkaVersion: String,portTopics: Int, dirSchema: String, dirTopics: String, clusterName: String, listFilesTopicsFromRepo: List[File], schemaRegistered : Map[String, Map[Int,String]]) : Unit=
  {
    try {
      //Get List of topics on Kafka
      val topicsOnKafkaString = requestTopic.httpGetTopicsString()
      val topicsOnKafkaArray = transformHTTPGetOutputStringToArray(topicsOnKafkaString)

      // Topic Repo
      //TODO Need to improve parsing of yml file. Basic parsing right now
      val mapTopicConf = listFilesTopicsFromRepo.map(x => (x.getName.replace(".yml", ""), readFileTopic(x.toString))).toMap
      val topicRepoToBeRegistered = mapTopicConf.map(x => x._2("topic")).toArray

      //Make the diff
      val diffExtraSchemaToBeRegistered = topicRepoToBeRegistered.toSet.diff(topicsOnKafkaArray.toSet)
      val diffExtraTopicOnKafka = topicsOnKafkaArray.toSet.diff(topicRepoToBeRegistered.toSet)

      //if new topics, push
      if (diffExtraSchemaToBeRegistered.size != 0) {
        val mapNewTopicToCreatOnKafka = mapTopicConf.filterNot(x => x._1.contains(diffExtraSchemaToBeRegistered))
        if (mapNewTopicToCreatOnKafka.keySet.size != 0) {
          mapNewTopicToCreatOnKafka.foreach(x => {
            val topicName = x._2("topic")
            val partitions = x._2("partitions").toInt
            val replications = x._2("replication-factor").toInt
            val schema = x._2("schema")
            //Check if schema is registered, pull it from server, through an error
            val subjectsString = requestSchema.httpGetSubjectList()
            val subjectsArray1 = transformHTTPGetOutputStringToArray(subjectsString)
            if (subjectsArray1.contains(schema)) {
              //else create
              httpCreateTopicsOnKafkaAndCheckClusterExist(ipTopics, portTopicsKafkaManager, zkHosts, kafkaVersion, clusterName, topicName, partitions, replications)
            } else {
              println(s"FAILURE: Cannot create topic ${'"'}${topicName}${'"'}, schema ${'"'}${schema}${'"'} is not registered")
              throw new Exception(s"Cannot create topic ${'"'}${topicName}${'"'}, schema ${'"'}${schema}${'"'} is not registered")
            }
          })
        }
      } else {
        if (topicRepoToBeRegistered.length != 0) {
          println(s"WARNING: Topics already on Kafka: ${topicRepoToBeRegistered.mkString(", ")}")
        }
      }

      //if topics already exist check if properties are the same, if yes nothing to do, else update AddPartition only allowed for now
      val updateTopicIfChangeList = schemaRegistered.keySet.intersect(topicsOnKafkaArray.toSet)
      //check number of partition in Kafka
      //Compare with number of partition in topic.yml file
      //if the same do nothing
      //if different update the number of partition

      //Delete topic if topic file is not there
      //ENFORCE A USER CONVENTION FOR TOPICS
      //USER.CUSTOMER -> NAME OF TOPIC, IF NOT THERE, DELETE
      val topicToDelete = diffExtraTopicOnKafka.filter(x => x.contains("user."))
      topicToDelete.foreach(x => requestTopic.httpDeletetTopic( clusterName, x))
    }catch{
      case e => throw new Exception(e.getMessage)
    }
  }



  //Check the folder topics/mytopic.yml verify that a schema has been registered in schemas/mytopic/mytopic.v1.yml
  def registerSchema( requestSchema: HttpRequestSchema, ip: String, port: Int, dirSchema: String, dirTopics: String) : Map[String, Map[Int, String]] ={
    try {
      val listFilesTopics = getListOfFiles(dirTopics)
      registerSchema(requestSchema, ip, port, dirSchema, listFilesTopics)
    }catch{
      case e => throw new Exception(e.getMessage)
    }
  }
  def registerSchema( requestSchema: HttpRequestSchema, ip: String, port: Int, dirSchema: String, listFilesTopics: List[File]) : Map[String, Map[Int, String]] ={
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
      case e => throw new Exception(e.getMessage)
    }
  }


/*
  case class VersionContent(json: String)
  case class SubjectValue( version: Map[Int, VersionContent])
  case class SubjectKey( name: String)
*/

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
    val fileContents = null
    val source= Source.fromFile(fileName)
    try {
      //val regex2 = """(?s)(.*)//""".r
      val t = source.getLines.map(_.replaceAll("\\s", "").replaceAll("\"", "").split(":"))
      val res=t.map( x => (x(0),x(1))).toMap
      res
    }finally {
      source.close() //closing file in JVM
      fileContents
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

  def httpCreateTopicsOnKafkaAndCheckClusterExist(ip: String, port: Int, zkHosts: String, kafkaVersion: String,  clusterName: String, topic: String, partitions: Int, replication: Int) : HttpResponse[String] ={
    var response= httpCreateTopicsOnKafka(ip, port, clusterName, topic, partitions, replication)

    if (response.toString.contains("Unknown cluster")) {
      println(s"Cluster ${clusterName} is not existing. We just created it")
      //Create Cluster
      val responseCluster = Http(s"http://${ip}:${port}/clusters")
        .postForm
        .param("name",clusterName)
        .param("zkHosts",zkHosts)//"zookeeper:2181"
        .param("kafkaVersion",kafkaVersion)//"0.9.0.1"
        .asString

      //then post (create topic)
      response = httpCreateTopicsOnKafka(ip, port, clusterName, topic, partitions, replication)
    }

    if(response.code == 200)
    {
      println(s"SUCCESS: Creation on Kafka of topic ${'"'}${topic}${'"'}" )
    }else{
      val parseJson =Json(response.body)
      val message = jget(parseJson, "message")
      println(s"FAILURE: Cannot create topic on Kafka ${'"'}${topic}${'"'} : ${message}")
    }
    response
  }

  def httpCreateTopicsOnKafka(ip: String, port: Int, clusterName: String, topic: String, partitions: Int, replication: Int) : HttpResponse[String] = {
    val response = Http(s"http://${ip}:${port}/clusters/${clusterName}/topics/create")
      .postForm
      .param("partitions", partitions.toString)
      .param("replication", replication.toString)
      .param("topic", topic)
      .asString

    response
  }

}



