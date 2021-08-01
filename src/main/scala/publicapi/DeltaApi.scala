package publicapi

import java.io.File

import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


object DeltaApi {
  case class Entity(id:String,name:String,value:Int,metaValue:Int)
  case class Stats(name:String,minValueString:String,maxValueString:String)
  val map:scala.collection.mutable.Map[String,List[Stats]] =scala.collection.mutable.Map()
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("databricks-sample1")
      .config("spark.default.parallelism", 4)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()

    import spark.implicits._
    spark.sessionState.conf.setConfString("spark.databricks.delta.stats.skipping","false")

    val mainDF = Seq(Entity("1","Sunny",34,2),
      Entity("2","Sunny",35,21),
      Entity("3","Sunny",36,3),
      Entity("4","Sunny",37,39),
      Entity("5","Sunny",38,22),
      Entity("6","Sunny",39,20),
      Entity("7","Sunny",41,42)).toDF()

    val footer = ParquetFileReader.readFooter(new Configuration(),new Path("/Users/sharadsingh/Dev/DeltaExperiment/p1/part-00000-055e1e3d-6d71-4e9f-873c-f07a3ea4b785-c000.snappy.parquet"))
    val jul:java.util.List[BlockMetaData] = footer.getBlocks
    val stats = jul.asScala.map(x=>x.getColumns.asScala.map(c=>{
      val columnName = c.getStatistics.`type`().getName
      val minValueString = c.getStatistics.minAsString()
      val maxValueString = c.getStatistics.maxAsString()
      Stats(columnName,minValueString,maxValueString)
    })).seq

    val dirPath = "/Users/sharadsingh/Dev/DeltaExperiment/p1"
    getFilePath(dirPath).map(prepareParquetStatMap(_))
    println("done")


    //mainDF.write.format("delta").mode("overwrite").save("./p1")

    //spark.sql("OPTIMIZE delta.`/p1`")getBlocks.get(0).getColumns.asScala.map(c=>c.getStatistics.`type`().getName)

  }

  def getFilePath(dirPath:String):Seq[String]={
    new File(dirPath).listFiles().
      filter(_.getAbsolutePath.endsWith(".parquet")).
      map(_.getAbsolutePath)
  }

  def prepareParquetStatMap(path:String):Unit={
    val footer = ParquetFileReader.readFooter(new Configuration(),new Path(path))
    val jul:java.util.List[BlockMetaData] = footer.getBlocks
    val stats = jul.asScala.flatMap(x=>x.getColumns.asScala.map(c=>{
      val columnName = c.getStatistics.`type`().getName
      val minValueString = c.getStatistics.minAsString()
      val maxValueString = c.getStatistics.maxAsString()
      Stats(columnName,minValueString,maxValueString)
    })).toList
    map.put(path,stats)
  }




}
