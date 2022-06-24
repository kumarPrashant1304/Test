package utilities

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object fileUtils {
  def readFile(path:String, format:String, options:Map[String, String], spark:SparkSession): DataFrame={
    val jsonDF = spark.read.format(format)
      .options(options)
      .load(path)
    println("In readJson function. Class of jsonDF : " + jsonDF.getClass)
    jsonDF
  }

  def writeFile(data:DataFrame, format:String, options:Map[String, String], path:String, mode:SaveMode): Unit ={
    data.write.format(format)
      .options(options)
      .mode(mode)
      .save(path)
  }

  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else prefix + "." + f.name

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".","_")))
      }
    })
  }
}
