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
public class AlibabaJob4Task3_subTask1 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("TestSparkSQL2").enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = spark.read().format("jdbc").option("url","jdbc:mysql://hadoopnode:3306/alibabadb")//mysql是库名
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "shop_info")//**user是表名
                .option("user", "hadoopuser").option("password", "hadoopuser").load();
        ds.createOrReplaceTempView("shop_info");
        spark.sparkContext().setLogLevel("WARN");

        spark.sql("use alibabadb");

        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","root");

        /*1、以浏览行为作为分析目标，输出2016.10.01~2016.10.31 共31 天的留存率，留存率在这里是指浏览用户的 剩余用户数/最初用户数*100%
        step1:条件日交易额前三的商家ID：*/
        spark.sql("select transDayPerYear.shop_id, totalPays,transDayPerYear ,totalPays/transDayPerYear AvgPays\n" +
                "from(\n" +
                "    select a.shop_id,count(1) transDayPerYear\n" +
                "     from(\n" +
                "         select  shop_id\n" +
                "         from user_view\n" +
                "         group by shop_id,DATE(time_stamp)\n" +
                "         having count(DATE(time_stamp))>=1\n" +
                "     ) as a\n" +
                "     group by a.shop_id\n" +
                ") as transDayPerYear join\n" +
                "(\n" +
                "    select a.shop_id, a.paycounts, b.per_pay*paycounts as totalPays\n" +
                "    from \n" +
                "        (select shop_id, count(1) as paycounts\n" +
                "        from user_pay\n" +
                "        group by shop_id) \n" +
                "    as a join shop_info b\n" +
                "    on a.shop_id=b.shop_id\n" +
                ") as totalPays\n" +
                "on transDayPerYear.shop_id=totalPays.shop_id\n" +
                "order by AvgPays desc\n" +
                "limit 3").createOrReplaceTempView("Top3ShopIDPerDay");

        /*step2：通过id查出时间范围内的用户浏览数*/
        spark.sql("select shop_id,DATE(time_stamp) as watchTime, count(1) as watchUsers  \n" +
                                    "from user_pay \n" +
                                    "where\n" +
                                    "shop_id in(    \n" +
                                    "    select shop_id from Top3ShopIDPerDay\n" +
                                    ")\n" +
                                    "and\n" +
                                    "DATE(time_stamp) >= '2016-10-01' and \n" +
                                    "DATE(time_stamp) <= '2016-10-31' \n" +
                                    "group by shop_id,DATE(time_stamp)\n" +
                                    "order by shop_id,watchTime").createOrReplaceTempView("temp1");
        /*step3：取出第一天的，取出所有天的，相除得到百分比*/
        spark.sql("select * from temp1 where watchTime = '2016-10-01'").createOrReplaceTempView("firstDay");
        spark.sql("select * from temp1 where watchTime >= '2016-10-01' and watchTime <= '2016-10-31'").createOrReplaceTempView("allDays");
        spark.sql("select b.shop_id,b.watchTime,b.watchUsers, CONCAT_WS('%',b.watchUsers/a.watchUsers*100,'')  retentionRate " +
                "          from firstDay a join allDays b " +
                "                           on a.shop_id=b.shop_id" +
                "           order by shop_id,watchTime").createOrReplaceTempView("retention_rate");
        spark.sql("select shop_id as shopId,watchTime,watchUsers,retentionRate from retention_rate")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb","retentionRate",prop);
        spark.close();
    }
}
