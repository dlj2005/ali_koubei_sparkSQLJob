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
public class AlibabaJob4Task3_subTask2 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("TestSparkSQL2").enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = spark.read().format("jdbc").option("url","jdbc:mysql://hadoopnode:3306/alibabadb")//mysql是库名
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "shop_info")//**user是表名
                .option("user", "hadoopuser").option("password", "hadoopuser").load();
        ds.createOrReplaceTempView("shop_info");

        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","root");

        spark.sql("use alibabadb");

        /*2、给定一个商店（可动态指定），输出该商店每天、每周和每月的被浏览数量，并选择合适的图表对结果进行可视化；
        step1: 天、周、月 实现不同的日期格式过滤：*/

        spark.sql("select a.shop_id, count(1) viewCounts, CONCAT_WS('-',YEAR(a.time_stamp),MONTH(a.time_stamp)) day\n" +
                "from user_view a join shop_info b\n" +
                "on a.shop_id=b.shop_id\n" +
//                "where a.shop_id='1197' \n" +
                "group by a.shop_id, CONCAT_WS('-',YEAR(a.time_stamp),MONTH(a.time_stamp))\n" +
                "order by a.shop_id, day")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb","month_view",prop);

        spark.sql("select a.shop_id, count(1) viewCounts, CONCAT_WS('-',YEAR(a.time_stamp),WEEKOFYEAR(a.time_stamp)) day\n" +
                "from user_view a join shop_info b\n" +
                "on a.shop_id=b.shop_id\n" +
//                "where a.shop_id='1197' \n" +
                "group by a.shop_id, CONCAT_WS('-',YEAR(a.time_stamp),WEEKOFYEAR(a.time_stamp))\n" +
                "order by a.shop_id, day")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb","week_view",prop);

        spark.sql("select a.shop_id, count(1) viewCounts, CONCAT_WS('-',MONTH(a.time_stamp),DAY(a.time_stamp)) day\n" +
                "from user_view a join shop_info b\n" +
                "on a.shop_id=b.shop_id\n" +
//                "where a.shop_id='1197' \n" +
                "group by a.shop_id, CONCAT_WS('-',MONTH(a.time_stamp),DAY(a.time_stamp))\n" +
                "order by a.shop_id, day")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb","day_view",prop);
                //todo:写入mysql时，可以配置插入mode，overwrite覆盖，append追加，ignore忽略，error默认表存在报错
                //resultDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.200.150:3306/spark","student",prop)
        spark.close();
    }
}
