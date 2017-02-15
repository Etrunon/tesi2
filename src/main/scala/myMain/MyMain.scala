package myMain

import java.io.File
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by etrunon on 13/02/17.
  */


class MyMain {

  def main(args: Array[String]): Boolean = {
    //    Sets spark Conf
    val sconf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("CommDec.Tesi2")
    val sc = new SparkContext(sconf)

    // Sets source folder
    val edgeFile = "/RundData/Input/mini1.csv"
    // Sets output folder
    val outputPath = "RunData/Output/" + new java.text.SimpleDateFormat("dd-MM-yyyy_HH:mm").format(new Date())
    val outputDir = new File(outputPath)
    outputDir.mkdirs()


  }
}
