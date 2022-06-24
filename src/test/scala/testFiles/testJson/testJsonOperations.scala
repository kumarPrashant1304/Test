package testFiles.testJson

import files.json.operateOnJsons
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import org.apache.spark.sql.SparkSession

@RunWith(classOf[JUnitRunner])
class testJsonOperations extends AnyFlatSpec with Matchers{
  val args:Array[String] = Array("Sample args Array")
  jsonTest()
  def jsonTest(): Unit = {
    "jsonTest_1" should "return Method is running fine" in {
      operateOnJsons.main(args) shouldNot be (1)
    }
  }
}