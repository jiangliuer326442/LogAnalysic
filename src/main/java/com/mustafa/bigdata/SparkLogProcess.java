package com.mustafa.bigdata;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

public class SparkLogProcess {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("GreetTask")
                .master("local")
                .getOrCreate();

        Dataset<Row> rowDf = spark.read().format("text").load("file:///home/mustafa/Documents/bigdata/jiazu/input");
        Encoder<GreetBean> greetBoEncoder = Encoders.bean(GreetBean.class);
        Dataset<GreetBean> rowGreetDf = rowDf.map(new MapFunction<Row, GreetBean>() {
            @Override
            public GreetBean call(Row value) throws Exception {
                GreetBean greetBo = new GreetBean();
                String rowContent = value.getString(0);

                String _time = rowContent.split(",")[0];
                greetBo.setTime(Timestamp.valueOf(_time));

                String jsonString = StringUtils.join(Arrays.copyOfRange(rowContent.split(","), 2, 4), ",");
                JsonArray jsonArray = new JsonParser().parse(jsonString).getAsJsonArray();

                String fromUid = jsonArray.get(0).toString().replaceAll("\"", "");
                greetBo.setFuid(fromUid);
                String toUid = jsonArray.get(1).toString().replaceAll("\"", "");
                greetBo.setTuid(toUid);

                return greetBo;
            }
        }, greetBoEncoder);

        rowGreetDf.registerTempTable("greetlog");

        Properties properties = new Properties();
        properties.put("user","hive_test");
        properties.put("password", "123456");
        String connectUrl = "jdbc:mysql://mustafa-PC:3306/jiazu?useSSL=false";

        Dataset<Row> groupsDataset = spark.read().jdbc(connectUrl, "`groups`", properties);
        groupsDataset.registerTempTable("groups");

        Dataset<Row> memberDataset = spark.read().jdbc(connectUrl, "`member`", properties);
        memberDataset.registerTempTable("member");

        Dataset<Row> memberSelectedDataSet = spark.sql("select m.uid from `groups` g left join member m on g.jzid = m.jzid where g.jzname = 'SP'");
        memberSelectedDataSet.registerTempTable("member_selected");

        Dataset<Row> result = spark.sql("select l.fuid, count(distinct tuid) as greet_nums from member_selected m right join greetlog l on m.uid = l.fuid group by fuid order by greet_nums desc limit 20");

        result.write().format("csv").save("file:///home/mustafa/Desktop/output");
//        result.foreach(new ForeachFunction<Row>() {
//            @Override
//            public void call(Row row) throws Exception {
//                System.out.println(row.toString());
//            }
//        });
    }
}
