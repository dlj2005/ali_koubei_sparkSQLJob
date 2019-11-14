package com.alibaba.wordofmouth.spark.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*alibaba口碑项目
任务4 批量分析
子任务1
平均日交易额最大的前10 个商家，并输出他们各自的交易额，并选择合适的图表对结果进行可视化；
关联两张表： shop_info (mysql)、user_pay(hive)
* */
public class AlibabaJob4Task1 {
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
        spark.sql("select transDayPerYear.shop_id, totalPays,transDayPerYear ,ROUND(totalPays/transDayPerYear,2) AvgPays\n" +
                "from(\n" +
                "    select a.shop_id,count(1) transDayPerYear\n" +
                "     from(\n" +
                "         select  shop_id\n" +
                "         from user_pay\n" +
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
                "limit 10")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb","top10avgpay",prop);
        spark.close();
    }
}
