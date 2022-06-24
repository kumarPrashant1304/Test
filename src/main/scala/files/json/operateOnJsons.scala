package files.json


import org.apache.spark.sql.{SaveMode, SparkSession}
import utilities.fileUtils


object operateOnJsons {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder
      .master("local[5]")
      .appName("JsonOperations")
      .config("spark.eventLog.dir", "file:///opt/spark/history/log")
      .config("spark.eventLog.enabled", value = true)
      .getOrCreate()
    val options:Map[String, String] = Map("multiline"->"true", "inferSchema"->"true")

    val fileDF = fileUtils.readFile("/home/prashant/sampleFiles/json/sample.json", "json", options, spark)
    fileDF.createOrReplaceTempView("sampleJson")
    val schema=fileDF.schema
    val flatSchema = fileUtils.flattenStructSchema(schema)
    val flatData = fileDF.select(flatSchema:_*)
    flatData.createOrReplaceTempView("flattenData")
    val flatDataSql = spark.sql("select * from sampleJson")
    val flattenDataSql = spark.sql("select * from flattenData")
    flatDataSql.show()
    println("schema tree for flatDataSql (sql query on the raw DF) : ")
    flatDataSql.printSchema()
    flatData.show()
    println("schema tree for flatData (Domain query with flatten schema) : ")
    flatData.printSchema()
    flattenDataSql.show()
    println("schema tree for flatData (sql query with flatten schema) : ")
    flattenDataSql.printSchema()
    val writeOption:Map[String,String] = Map[String,String]()
    fileUtils.writeFile(flatDataSql,"json",writeOption,"/home/prashant/sampleFiles/json/output/origFile.json",SaveMode.Overwrite)
    fileUtils.writeFile(flatData,"json",writeOption,"/home/prashant/sampleFiles/json/output/domainFlatData.json",SaveMode.Overwrite)
    fileUtils.writeFile(flattenDataSql,"json",writeOption,"/home/prashant/sampleFiles/json/output/sqlFlatData.json",SaveMode.Overwrite)
    spark.stop()
  }
}
