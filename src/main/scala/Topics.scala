import java.io.File

import FilesUtils.{readFileTopic, transformHTTPGetOutputStringToArray}

class Topics( ipTopics: String, portTopicsKafkaManager: Int, portTopics: Int, folder: String, cluster: String, zkHosts: String, kafkaVersion: String) {

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
              requestTopic.httpCreateTopicsOnKafkaAndCheckClusterExist(zkHosts, kafkaVersion, clusterName, topicName, partitions, replications)
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


}
