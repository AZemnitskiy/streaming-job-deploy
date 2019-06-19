import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class SchemaRegistryTest extends FunSuite {
  val pathCreating = System.getProperty("user.dir")+File.separator+ "src"+File.separator+"test"+File.separator+"resources"+File.separator+"streaming-job-workflow"
  val path= pathCreating.replaceAll("\\\\","/")
  val conf = ConfigFactory.load()
  val ip = conf.getString("schemas.host-ip")
  val port = conf.getInt("schemas.host-port")


  val requestSchema = new HttpRequestSchema(ip, port )
  test("SchemaRegistry.MissingSchemaTopicsYMLPresent") {
    //All schema present need to be register if they can
    println("MissingSchemaTopicsYMLPresent")
    //Added topics shampoo.yml,but no schema file for it
    val dirSchema = s"${path}/missing-schema-for-topics/schemas"
    val dirTopics = s"${path}/missing-schema-for-topics/topics"


    Deploy.registerSchema(requestSchema, ip, port, dirSchema, dirTopics)

    val subjectsString = requestSchema.httpGetSubjectList()
    assert(subjectsString.contains("customer"))
    assert(subjectsString.contains("product"))

    println("")

  }

  test("SchemaRegistry.ExtraSchemaNoTopicsYMLAssociated") {
    println("ExtraSchemaNoTopicsYMLAssociated")
    //ExtraSchemaNoTopicsYMLAssociated, in this case schema is still register on Kafka
    //Product schema is present, but no product.yml in topics
    val conf = ConfigFactory.load()
    val ip = conf.getString("schemas.host-ip")
    val port = conf.getInt("schemas.host-port")
    val dirSchema = s"${path}/extra-schema-no-topics/schemas"//conf.getString("schemas.folder")
    val dirTopics = s"${path}/extra-schema-no-topics/topics"

    Deploy.registerSchema( requestSchema, ip, port, dirSchema, dirTopics)
    val subjectsString = requestSchema.httpGetSubjectList()//io.Source.fromURL(s"http://${ip}:${port}/subjects").mkString

    assert(subjectsString.contains("product"))
    assert(subjectsString.contains("customer")) //test subject correctly push on Kafka
  }

}