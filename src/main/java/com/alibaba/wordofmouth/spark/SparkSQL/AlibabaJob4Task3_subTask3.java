package com.alibaba.wordofmouth.spark.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*alibaba口碑项目
任务4 批量分析
子任务3
1、平均日交易额最大的前3 个商家
以浏览行为作为分析目标，输出2016.10.01~2016.10.31 共31 天的留存率，留存率在这里是指浏览用户的流失率

2、给定一个商店（可动态指定），输出该商店每天、每周和每月的被浏览数量，并选择合适的图表对结果进行可视化；

3、找到被浏览次数最多的50 个商家，并输出他们的城市以及人均消费，并选择合适的图表对结果进行可视化。
* */
public class AlibabaJob4Task3_subTask3 {


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("TestSparkSQL2").enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = spark.read().format("jdbc").option("url","jdbc:mysql://hadoopnode:3306/alibabadb")//mysql是库名
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "shop_info")//**user是表名
                .option("user", "hadoopuser").option("password", "hadoopuser").load();
        ds.createOrReplaceTempView("shop_info");

        spark.sql("use alibabadb");

        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","root");

        /*3、找到被浏览次数最多的50 个商家，并输出他们的城市以及人均消费，并选择合适的图表对结果进行可视化。
        step1: ：*/

        spark.sql("select a.shop_id, count(1) views\n" +
                "from user_view a join shop_info b\n" +
                "on a.shop_id=b.shop_id\n" +
                "group by a.shop_id\n" +
                "limit 50").createOrReplaceTempView("Top50ViewsShop");

        spark.sql("select a.shop_id, a.paycounts, a.payPersons, b.cate_3_name, b.city_name, b.per_pay, b.score, b.per_pay*paycounts/a.payPersons as AvgPay\n" +
                "                       from\n" +
                "                      (select shop_id as shop_id , count(1) as paycounts,count(distinct(user_id)) as payPersons\n" +
                "                      from user_pay group by shop_id) as a\n" +
                "                      join shop_info b\n" +
                "                      on a.shop_id=b.shop_id\n" +
                "                      join Top50ViewsShop c \n" +
                "                       on a.shop_id=c.shop_id" +
                "                       ")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb?useUnicode=true&characterEncoding=UTF-8","top50views",prop);
        spark.close();
    }
}
