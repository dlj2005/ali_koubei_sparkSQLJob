package com.alibaba.wordofmouth.spark.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/*alibaba口碑项目
任务4 批量分析
子任务2
输出北京、上海、广州和深圳四个城市最受欢迎的5 家奶茶商店和中式快餐编号（这两个分别输出出来）并选择合适的图表对结果进行可视化（类似排行榜）
关联两张表： shop_info (mysql)、user_pay(hive)
hive存在中文查询问题，增加了城市编码和食品编码以解决该问题，后续实现前台展示时，表单选项采用下拉列表方式，用户选择的是中文，传入后台的是对应的编码，以解决该问题
* */
public class AlibabaJob4Task2_1 {

    static int[] cate_3_code = {01,02} ;  //01奶茶、02中式快餐

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("TestSparkSQL2").enableHiveSupport().getOrCreate();
        Dataset<Row> ds = spark.read().format("jdbc").option("url","jdbc:mysql://hadoopnode:3306/alibabadb")//mysql是库名
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "shop_info")//**user是表名
                .option("user", "hadoopuser").option("password", "hadoopuser").load();
        ds.createOrReplaceTempView("shop_info");

        spark.sql("use alibabadb");

        Properties prop = new Properties();
        prop.setProperty("user","root");
        prop.setProperty("password","root");


        /*平均每人消费金额*/
        spark.sql("select a.shop_id, a.paycounts, a.payPersons, b.cate_3_name, b.city_name, b.per_pay, b.score, b.per_pay*paycounts/a.payPersons as AvgPay\n" +
                "         from" +
                "        (select shop_id as shop_id , count(1) as paycounts,count(distinct(user_id)) as payPersons\n" +
                "        from user_pay group by shop_id) as a\n" +
                "        join shop_info b\n" +
                "        on a.shop_id=b.shop_id\n" +
                "   where\n" +
                "       (b.city_code=010 or\n" +
                "        b.city_code=020 or\n" +
                "        b.city_code=021 or\n" +
                "        b.city_code=0755) and\n" +
                "        b.cate_3_code="+cate_3_code[0]).createOrReplaceTempView("per_paynumbers");

        /*用户最高消费金额*/
        spark.sql("select user_pay.shop_id as shop_id, user_pay.user_id,bb.per_pay, bb.score ,count(user_pay.user_id) as usermaxpaycounts, bb.per_pay*count(user_pay.user_id) userMaxTotalpay\n" +
                "        from user_pay  join per_paynumbers bb\n" +
                "        on user_pay.shop_id=bb.shop_id\n" +
                "        group by user_pay.user_id, user_pay.shop_id, bb.per_pay, bb.score \n" +
                "        order by userMaxTotalpay desc").createOrReplaceTempView("maxpaylist");

        /*商户评分与最高消费金额*/
        spark.sql("select shop_id, score,max(userMaxTotalpay) MaxPay\n" +
                "        from maxpaylist\n" +
                "        group by shop_id,score").createOrReplaceTempView("shopScoreMaxpay");

        /*最终公式计算结果*/
        spark.sql("select aa.shop_id, aa.city_name, aa.cate_3_name as foodname, ROUND(0.7*(dd.score/5)+0.3*(aa.AvgPay/dd.MaxPay),2) popInex\n" +
                "from per_paynumbers aa join shopScoreMaxpay dd\n" +
                "on aa.shop_id=dd.shop_id\n" +
                "order by popInex desc\n" +
                "limit 5")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoopnode:3306/alibabadb?useUnicode=true&characterEncoding=UTF-8","topPopFood",prop);

        spark.close();
    }
}
