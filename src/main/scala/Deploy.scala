import com.typesafe.config.ConfigFactory
import java.io.File
import scalaj.http.{Http, HttpResponse}
import scala.io.Source
import com.persist.JsonOps._



object Deploy {

  def main(args: Array[String] ): Unit = {
    val path= args(1)//"C:/files/workspace_spark/streaming-jobs-workflow/"//args(0)

    val myConfigFile = new File(args(0))//"C:/files/workspace_spark/Deploy/target/scala-2.12/prod.conf"
    val conf = ConfigFactory.parseFile(myConfigFile)

    val ip = conf.getString("schemas.host-ip")
    val port = conf.getInt("schemas.host-port")
    val dirSchema = path + conf.getString("schemas.folder")

    val ipTopics = conf.getString("topics.host-ip")
    val portTopicsKafkaManager = conf.getInt("topics.host-port-kafka-manager")
    val portTopics = conf.getInt("topics.host-port")
    val dirTopics = path + conf.getString("topics.folder")
    val clusterName = conf.getString("topics.cluster")
    val zkHosts = conf.getString("topics.zkHosts")
    val kafkaVersion = conf.getString("topics.kafkaVersion")

    val listFilesTopicsFromRepo = getListOfFiles(dirTopics)

    //##############Register Schema #################
    val schemaRegistered =registerSchema( ip, port, dirSchema, listFilesTopicsFromRepo)
    //schemaRegistered.foreach(println)

    //##############Publish Topics #################
    //Once Schema is registered, push topics on Kafka:
    //-if topics already exist update it
    //-if new topics, publish it
    createOrUpdateTopics( ipTopics, portTopicsKafkaManager, zkHosts: String, kafkaVersion: String,portTopics, dirSchema, dirTopics, clusterName, listFilesTopicsFromRepo, schemaRegistered)

  }

  case class TopicsConf( ip: String, portTopicsKafkaManager: Int, port: Int, folder: String, cluster: String, zkHosts: String, kafkaVersion: String)
  case class SchemasConf( ip: String, port: Int, dirSchema: String)


  def createOrUpdateTopics(ipTopics: String, portTopicsKafkaManager: Int, zkHosts: String, kafkaVersion: String,portTopics: Int, dirSchema: String, dirTopics: String, clusterName: String, listFilesTopicsFromRepo: List[File], schemaRegistered : Map[String, Map[Int,String]]) : Unit=
  {
    //Get List of topics on Kafka
    val topicsOnKafkaString = io.Source.fromURL(s"http://${ipTopics}:${portTopics}/topics/").mkString
    val topicsOnKafkaArray = transformHTTPGetOutputStringToArray(topicsOnKafkaString)
    //topicsOnKafkaArray.foreach(println)

   

   // println("List of topics file on repository: " + listFilesTopicsFromRepo)
    //TODO Need to improve parsing of yml file. Basic parsing right now
    val mapTopicConf= listFilesTopicsFromRepo.map( x => (x.getName.replace(".yml",""), readFileTopic(x.toString))).toMap
    val topicRepoToBeRegistered =mapTopicConf.map(x=> x._2("topic")).toArray

    //Make the diff
    val diffExtraSchemaToBeRegistered = topicRepoToBeRegistered.toSet.diff(topicsOnKafkaArray.toSet)
    //println("Topic to be registered on Kafka. (Schema has been registered): "+diffExtraSchemaToBeRegistered)
    val diffExtraTopicOnKafka = topicsOnKafkaArray.toSet.diff(topicRepoToBeRegistered.toSet)
    //println("Extra Topic on Kafka: "+diffExtraTopicOnKafka)
    
    //if new topics, push
    val mapNewTopicToCreatOnKafka = mapTopicConf.filterNot(x => x._1.contains(diffExtraSchemaToBeRegistered))
    if (mapNewTopicToCreatOnKafka.keySet.size!=0) {
      mapNewTopicToCreatOnKafka.foreach(x => {
        val topicName = x._2("topic")
        val partitions = x._2("partitions").toInt
        val replications = x._2("replication-factor").toInt
        httpCreateTopicsOnKafkaAndCheckClusterExist(ipTopics, portTopicsKafkaManager, zkHosts, kafkaVersion, clusterName, topicName, partitions, replications)
      })
    }

    //if topics already exist check if properties are the same, if yes nothing to do, else update AddPartition only allowed for now
    val updateTopicIfChangeList = schemaRegistered.keySet.intersect(topicsOnKafkaArray.toSet)
   // println(updateTopicIfChangeList)
    //check number of partition in Kafka
    //Compare with number of partition in topic.yml file
    //if the same do nothing
    //if different update the number of partition

    //Delete topic if topic file is not there
    //ENFORCE A USER CONVENTION FOR TOPICS
    //uSER.CUSTOMER -> NAME OF TOPIC, IF NOT THERE, DELETE
    //TODO TopicToDelete is wrong!!! delete too many files because of the new notation user.customer for topic, it thinks topic is customer line 54
    val topicToDelete = diffExtraTopicOnKafka.filter( x=> x.contains("user."))
    topicToDelete.foreach( x=>httpDeletetTopic( ipTopics, portTopicsKafkaManager, clusterName, x))
  }

  def httpDeletetTopic( ipTopics:String, portTopicsKafkaManager:Int, clusterName:String, topicName: String ): HttpResponse[String] = {
    val response = Http(s"http://${ipTopics}:${portTopicsKafkaManager.toString}/clusters/${clusterName}/topics/delete?t=${topicName}")
      .postForm
      .param("topic",topicName)
      .asString

    if(response.code == 200) {
      println("SUCCESS: Delete topic: " + topicName)
    }else{
      println("FAILURE: Did not succeed to delete \""+topicName+"\"")
      val parseJson =Json(response.body)
      println("FAILURE: "+ jget(parseJson, "message"))
    }
    response
  }

  //Check the folder topics/mytopic.yml verify that a schema has been registered in schemas/mytopic/mytopic.v1.yml
  //if yes, register the new schema to Kafka registry
  //If not it send an error message : “Missing schema file for this topic in schemas folder”
  //If there exist some topics in schema whithout yml file disregard and send a warning: “Topic mytopic not push to kafka as no yml file in topics associated with”
  def registerSchema(  ip: String, port: Int, dirSchema: String, dirTopics: String) : Map[String, Map[Int, String]] ={
    val listFilesTopics = getListOfFiles(dirTopics)
    registerSchema(  ip, port, dirSchema, listFilesTopics)
  }
  def registerSchema(  ip: String, port: Int, dirSchema: String, listFilesTopics: List[File]) : Map[String, Map[Int, String]] ={
    val listTopics = listFilesTopics.map(x => x.getName.replace(".yml", ""))

    //REPOSITORIES
    // var schemas_states:Map[String,Map[Int,String]] = Map()
    val listDirSchemas = getListOfDir(dirSchema)
    val schemas_states= populateSubjects(listDirSchemas)
    var schemasStatesWanted = schemas_states
    //println("///Schema_states: "+schemas_states)
    var mapSubjectVersionRepo = schemas_states.map(x => (x._1,x._2.map(v=>v._1).toList))

    val listSchemaRepoPresent =mapSubjectVersionRepo.map(x => if(x._2.length!=0){x._1}else {
      println(s"WARNING: This folder ${x._1} is empty")
      mapSubjectVersionRepo = mapSubjectVersionRepo.filterKeys(k => k!=x._1)
      schemasStatesWanted = schemasStatesWanted.filterKeys(k => k!=x._1)
    })

    val listRepoToCompareTopics = listSchemaRepoPresent.map(x => x.toString).filter( x=> x!="()")
    val diffExtraSchema = listRepoToCompareTopics.toSet.diff(listTopics.toSet)
    val diffTopicWithNoSchema = listTopics.toSet.diff(listRepoToCompareTopics.toSet)

    if (diffTopicWithNoSchema.size>0){
      println("ERROR: Missing schema file for these topics:")
      diffTopicWithNoSchema.foreach(println)
      throw new Exception("Missing Schema for these topic")
    }
    if (diffExtraSchema.size>0){
      println(s"WARNING:  ${diffExtraSchema} do not have corresponding topics.yml. Schema will be ignored.")
      diffExtraSchema.foreach( x => mapSubjectVersionRepo = mapSubjectVersionRepo.filterKeys(_!=x))
      diffExtraSchema.foreach( x => schemasStatesWanted = schemasStatesWanted.filterKeys(_!=x))

    }
    //println("REPO: List of subject and version: " + mapSubjectVersionRepo)
    //println("REPO: List of subject and version and content: " + schemasStatesWanted)

    //KAFKA
    //list subject Kafka
    val subjectsString = io.Source.fromURL(s"http://${ip}:${port}/subjects").mkString

    val subjectsArray = transformHTTPGetOutputStringToArray( subjectsString)

    //if subject list is empty in Kafka
    var mapSubjectVersionKafka = Map.empty[String, List[Int]]
    if (subjectsArray(0)!="") {
      val arraySubjectVersion = subjectsArray.map(x => (x, io.Source.fromURL(s"http://${ip}:${port}/subjects/${x}/versions").mkString))
      val listSubjectVersionKafka = arraySubjectVersion.map(x => (x._1, x._2.replace("[", "").replace("]", "").split(",").toList)).toMap
      mapSubjectVersionKafka = listSubjectVersionKafka.map(x => (x._1, x._2.map(_.toInt)))
    }

    //println("KAFKA: List of subject and version: " + mapSubjectVersionKafka) //Map[String, List[Int]]

    //Extra in Repo
    val diffExtraRepo : Map[String, List[Int]] = mapSubjectVersionRepo -- mapSubjectVersionKafka.keySet
    //println("Extra in Repo: " + diffExtraRepo)

    //Extra in Kafka
    val diffExtraKafka : Map[String, List[Int]] = mapSubjectVersionKafka -- mapSubjectVersionRepo.keySet
    //println("Extra in Kafka: " + diffExtraKafka)

    //Delete any subject extra in kafka
    diffExtraKafka.foreach( x=>httpDeletetSchemaRegistryAndAllVersion( x._1, ip, port ) )

    //Post any subject extra in Repo  to Kafka
   // println("Access Map:" + schemas_states("product"))
    schemasStatesWanted.foreach( x=> httpPostSchemaRegistryForEachVersion(x._1,x._2,ip,port) )

    //println("Schema State Wanted: " + schemasStatesWanted)
    schemasStatesWanted
  }

  def httpPostSchemaRegistryForEachVersion( subject:String, versionData:Map[Int,String], ip:String, port:Int ): Unit = {
    versionData.foreach( versionDataMap => {
      val t =versionDataMap._2.replace("\"", "\\\"")
      val postData ="{\"schema\": \""+ t +"\"}\n"
      httpPostSchemaRegistry(subject, postData ,ip,port)
    } )
  }

    def httpPostSchemaRegistry( subject:String, postData:String, ip:String, port:Int ): HttpResponse[String] = {
      val response = Http(s"http://${ip}:${port}/subjects/${subject}/versions")///post
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .postData(postData)
        .asString

      if(response.code == 200) {
        println("SUCCESS: Posting schema registry for subject: " + subject)
       // println("Http request post response: " + response)
      }else{
        if(response.code == 409){
          println("FAILURE: Issue to post subject: " + subject + ". Error message below.")
          val parseJson =Json(response.body)
          println("FAILURE: "+ jget(parseJson, "message"))
        }else{
        println("FAILURE: Issue to post subject: " + subject)
          val parseJson =Json(response.body)
          println("FAILURE: "+ jget(parseJson, "message"))
        }
      }
      response
    }

    def httpDeletetSchemaRegistryAndAllVersion( subject:String, ip:String, port:Int ): HttpResponse[String] = {
      val response = Http(s"http://${ip}:${port}/subjects/${subject}/")
        .method("DELETE")
        .asString

      if (response.code == 200) {
        println("Delete schema registry and all version of: " + subject)
        //println("Http request delete response:" + response)
      }else
      {
        println("FAILURE: Issue to delete subject: " +subject)}
        val parseJson =Json(response.body)
        println("FAILURE: "+ jget(parseJson, "message"))
        response
    }


  //go to folder and list all folder inside folder
  //foreach folder go inside it and get all the file
  //Case class subject , another one version
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
    val fileContents = null
    val source= Source.fromFile(fileName)
    try {
      val fileContents = source.getLines.mkString
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
      println("SUCCESS: Creation on Kafka of topic :" + topic )
    }else{
      println("FAILURE: Issue to create on kafka topic:" + topic)
      val parseJson =Json(response.body)
      println("FAILURE: "+ jget(parseJson, "message"))
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



