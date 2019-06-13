# Deploy Example:
## Register Avro Schema & Push data on Kafka bus Via REST API 

# Build Scala Jar
````
sbt assembly
````

# Run Example
java -jar .\Deploy-assembly-0.1.jar productionConfigFilePath FolderLocation
````
java -jar .\Deploy-assembly-0.1.jar "C:/files/workspace_spark/Deploy/target/scala-2.12/prod.conf" "C:/files/workspace_spark/streaming-jobs-workflow/"
````
 
Example of prod.conf file:
````
schemas : {
  host-ip: "192.168.99.100"
  host-port: 8081
  folder: "schemas"
}

topics : {
  host-ip: "192.168.99.100"
  host-port-kafka-manager: 8004
  host-port: 8084
  folder: "topics"
  cluster: "myCluster"
  zkHosts:"zookeeper:2181"
  kafkaVersion: "0.9.0.1"
}

jobs : {
  host-ip: "192.168.99.100"
  host-port: 8083
  folder: "topics"
}
````
 
 
# Schema Registration 
 
 Run SchemaRegistryTest.scala to test Schema registration.

## Missing avro schema for topic  
If a topics.yml is created without an associated Avro schema, the code output an exception.
The test SchemaRegistry.MissingSchemaTopicsYMLPresent shows that.
Per the below folder structure, you can see that a shampoo.yml file has been created but no avro schemas.

````
topics
  customer.yml
  shampoo.yml
schemas
  customer
    customer.v1.avsc
    customer.v2.avsc
````

## Extra schema with no topics
If there is an extra schema with no topics, the schema is ignore and not register to kafka.
The test SchemaRegistry.ExtraSchemaNoTopicsYMLAssociated shows that.
Per the below folder structure, you can see that product.avsc file has been created but no topic.

````
topics
  customer.yml
schemas
  customer
    customer.v1.avsc
    customer.v2.avsc
  product
    product.v1.avsc  
````
The output on schema registry should not contain product.
In this case, avro schema registry should be ["customer"].



 # Create and Update Topic on Kafka
 ## Create Topic
 Only topic that is not already existing on Kafka can be created. If they already exist please see Update.
 As mention before, topic will be created only if they have avro Schema associated.

 Run example TopicCreateTest.scala to see an example of topic creation.

Note: For now we are not considering the schema version in the topic.yml. We are just publishing any schemas versions that 
is associated with file title topic.yml

 ## Update Topic on Kafka
 We are allowing for now only to add partition.
 This feature is not yet implemnented. TODO.