package com.zhyl.medical.function

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class OperateTable(spark: SparkSession) {

  import spark.implicits._

  def relation(date: String): Map[String, String] = {
    spark.read.table("ods.dict_full")
      .where($"dt" === date)
      .select("id", "value")
      .as[(String, String)]
      .collect()
      .toMap
  }

  def myMatching(map: Map[String, String]): UserDefinedFunction = {
    udf((col: String) => map.getOrElse(col, col))
  }

  // TODO：添加年龄字段
  def addAgeGroupColumn(df: DataFrame, ageColumn: Column): DataFrame = {
    df.withColumn("age_group",
      when(ageColumn.between(0, 2), "婴儿期")
        .when(ageColumn.between(3, 5), "幼儿期")
        .when(ageColumn.between(6, 11), "小学阶段")
        .when(ageColumn.between(12, 17), "青少年期")
        .when(ageColumn.between(18, 29), "青年期")
        .when(ageColumn.between(30, 59), "中年期")
        .when(ageColumn.between(60, 122), "老年期")
        .otherwise("年龄异常")
    )
  }

  // TODO:计算年龄，并且添加范围字段
  def calculateAgeAndGroup(patientDF: DataFrame, date: String): DataFrame = {
    // 使用 months_between 函数进行准确的年龄计算
    val patientWithAgeDF = patientDF.withColumn(
      "age",
      (months_between(lit(date), $"birthday") / 12).cast("int")
    )
    // 调用 addAgeGroupColumn 函数，基于年龄添加年龄组字段
    addAgeGroupColumn(patientWithAgeDF, $"age")
  }

  // TODO:分组统计
  def groupCount(df: DataFrame, groupField: String, alias: String, countField: String = "*"): DataFrame = {
    df.groupBy(groupField)
      .agg(count(countField) as alias)
  }

  // TODO:分租求和
  def groupSum(df: DataFrame, groupField: String, sumField: String, alias: String): DataFrame = {
    df.groupBy(groupField)
      .agg(sum(sumField) as alias)
  }

  def countFunc(df: DataFrame, fieldName: String): DataFrame = {
    df.agg(count("*") as (fieldName))
  }


  // TODO: 将月份转换为季节的函数
  val monthToSeasonUDF: UserDefinedFunction = udf((month: Int) => {
    month match {
      case m if 3 to 5 contains m => "春季"
      case m if 6 to 8 contains m => "夏季"
      case m if 9 to 11 contains m => "秋季"
      case 12 | 1 | 2 => "冬季"
      case _ => "未知季节"
    }
  })

}
