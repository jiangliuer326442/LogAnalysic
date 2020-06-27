package com.mustafa.bigdata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;

/**
 * 把csv数据导入到 hive 表中
 * 先使用sqoop将mysql数据导入到hive中
 * 将hive lib中 hive-common 和 hive-exec 的jar包拷贝到sqoop中
bin/sqoop import \
--connect jdbc:mysql://mustafa-PC:3306/jiazu \
--username root \
--password 123456 \
--columns jzid,jzname \
--table groups \
--num-mappers 1 \
--mapreduce-job-name jiazu_groups_to_hive \
--fields-terminated-by '\t' \
--target-dir /user/mustafa/hive/tables/jiazu/groups \
--delete-target-dir \
--hive-import \
--create-hive-table \
--hive-database jiazu \
--hive-overwrite \
--hive-table groups

bin/sqoop import \
--connect jdbc:mysql://mustafa-PC:3306/jiazu \
--username root \
--password 123456 \
--columns jzid,uid \
--table member \
--num-mappers 1 \
--mapreduce-job-name jiazu_member_to_hive \
--fields-terminated-by '\t' \
--target-dir /user/mustafa/hive/tables/jiazu/member \
--delete-target-dir \
--hive-import \
--create-hive-table \
--hive-database jiazu \
--hive-overwrite \
--hive-table member
 *
 * csv 导入到表后，执行以下sql语句进行统计
 *
 * $sql1 = select m.uid from groups g left join member m on g.jzid = m.jzid where g.jzname = 'SP'
 *
 * $sql2 = select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from im where fuid in ($sql1) and time >= '2020-06-23 00:00:00' and time < '2020-06-24 00:00:00';
 */


public class LoadCsv2Table {

    public static void main(String[] args) throws UnknownHostException, SQLException, ClassNotFoundException {
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbc_url = "jdbc:hive2://" + hostname + ":10000/jiazu";
        Connection conn = DriverManager.getConnection(jdbc_url,"mustafa", null);
        Statement st = conn.createStatement();

        st.execute("create table if not exists im (\n" +
                "    id int,\n" +
                "    fuid int,\n" +
                "    tuid int,\n" +
                "    time TIMESTAMP\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t'");
        st.execute("load data inpath '/user/mustafa/bigdata/jiazu/output/im' into table im");
        st.execute("create external table if not exists video (\n" +
                "    id int,\n" +
                "    fuid int,\n" +
                "    tuid int,\n" +
                "    time TIMESTAMP\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t'");
        st.execute("load data inpath '/user/mustafa/bigdata/jiazu/output/video' into table video");

        StringBuilder uidsStringBuilder = new StringBuilder("");
        ResultSet resultset = st.executeQuery("select m.uid from groups g left join member m on g.jzid = m.jzid where g.jzname = 'SP'");
        while(resultset.next()) {
            uidsStringBuilder.append(resultset.getString("uid")).append(",");
        }
        String uids = uidsStringBuilder.toString().substring(0, uidsStringBuilder.toString().length()-1);

        st.execute("insert overwrite local directory '/home/mustafa/Desktop/im/im-2020-06-23' row format delimited fields terminated by ',' select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from im where fuid in (" + uids + ") and time >= '2020-06-23 00:00:00' and time < '2020-06-24 00:00:00'");
        st.execute("insert overwrite local directory '/home/mustafa/Desktop/im/im-2020-06-24' row format delimited fields terminated by ',' select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from im where fuid in (" + uids + ") and time >= '2020-06-24 00:00:00' and time < '2020-06-25 00:00:00'");
        st.execute("insert overwrite local directory '/home/mustafa/Desktop/im/video-2020-06-23' row format delimited fields terminated by ',' select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from video where fuid in (" + uids + ") and time >= '2020-06-23 00:00:00' and time < '2020-06-24 00:00:00'");
        st.execute("insert overwrite local directory '/home/mustafa/Desktop/im/video-2020-06-24' row format delimited fields terminated by ',' select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from video where fuid in (" + uids + ") and time >= '2020-06-24 00:00:00' and time < '2020-06-25 00:00:00'");

        st.close();
        conn.close();
    }

}
