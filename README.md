# Hadoop+Hive+Sqoop 离线日志分析 公会女生打招呼数据

## 需求背景：

我们将女生主动和男生建立联系定义为女生打招呼，app中女生打招呼的方式有两种：主动发起文字聊天和主动发起音视频聊天。这些数据的采集通过在应用程序中增加埋点，最终成为日志文件保存在服务器上。日志内容如下：

文字聊天，日志文件 social_talklist_im_2020-06-23.log，内容示例如下：

```verilog
2020-06-23 23:59:44,10.3.1.32,[8988487,9050759]
2020-06-23 23:59:47,10.3.1.32,[9016540,8946882]
2020-06-23 23:59:47,10.3.1.32,[9011059,9050680]
```

后两段内容含义是 发起文字聊天的女生uid和接收文字消息的男生uid。

音视频聊天，日志文件 social_talklist_video_2020-06-23.log，内容示例如下：

```verilog
2020-06-23 23:59:33,10.3.1.34,["8935739",8808419]
2020-06-23 23:59:55,10.3.1.20,["9037381",9050732]
```

**需求是，以指定公会为例，统计该公会某一天通过文字打招呼的女生数量，人均打招呼的次数，打招呼的男生数量等数据**

## 分析：

hive的 sql on hadoop 首先需要我们有一个csv格式的文件，我们可以将他导入到hive表中，通过sql语句自动生成hadoop的mapreduce任务执行。

csv文件从那里来？我们首先需要将日志文件上传到hdfs文件系统。写一个只有map的mapreduce程序，将日志文件解析成csv文件。

此外 家族表和公会成员表存储在mysql中，我们需要将他导入到hive表中，一遍进行联表查询。

## 实现：

### 1. 将日志文件上传到hdfs文件系统

gradle文件引入依赖：`implementation "org.apache.hadoop:hadoop-client:$hadoopVersion"`

```bash
 gradle installDist //打成可执行文件
 cd build/install/LogAnalysic //进入执行目录
 bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_im_2020-06-23.log bigdata/jiazu/input/im/2020-06-23.log //上传日志文件
 bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_im_2020-06-24.log bigdata/jiazu/input/im/2020-06-24.log
 bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_video_2020-06-23.log bigdata/jiazu/input/video/2020-06-23.log
 bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_video_2020-06-24.log bigdata/jiazu/input/video/2020-06-24.log

```

代码参照以下链接：[hdfs上传文件](https://github.com/jiangliuer326442/LogAnalysic/blob/master/src/main/java/com/mustafa/bigdata/Upload2Hdfs.java)

### 2. 将日志文件解析成csv文件

我们需要将项目打成jar包，在yarn上跑mapreduce程序

gradle额外需要引入依赖：`    implementation 'com.google.code.gson:gson:2.6.2'
    implementation 'commons-lang:commons-lang:2.6'`

```bash
gradle jar
HADOOP_HOME=/data/home/software/hadoop-2.6.0-cdh5.16.2
$HADOOP_HOME/bin/yarn jar \
build/libs/LogAnalysic-1.0-SNAPSHOT.jar \
com.mustafa.bigdata.ParseLog2Csv bigdata/jiazu/input/im bigdata/jiazu/output/im

$HADOOP_HOME/bin/yarn jar \
build/libs/LogAnalysic-1.0-SNAPSHOT.jar \
com.mustafa.bigdata.ParseLog2Csv bigdata/jiazu/input/video bigdata/jiazu/output/video
```

代码参照以下链接：[日志解析成csv文件](https://github.com/jiangliuer326442/LogAnalysic/blob/master/src/main/java/com/mustafa/bigdata/ParseLog2Csv.java)

### 3. 将mysql中家族表和家族成员表导入hive中

```bash
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
```

### 4. 使用hive进行离线日志的分析

主要是以下两个sql语句：

```mysql
select m.uid from groups g left join member m on g.jzid = m.jzid where g.jzname = 'aaa'
select count(fuid) as greet_times, count(distinct fuid) as greet_users, count(distinct tuid) as disturb_users from im where fuid in ($sql1) and time >= '2020-06-23 00:00:00' and time < '2020-06-24 00:00:00'
```

gradle中引入依赖：`implementation "org.apache.hive:hive-jdbc:$hiveVersion"`

代码：

```java
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
```

数据分析的结果保存在本地的 `/home/mustafa/Desktop/im`路径下

详细的代码请参照以下链接：

[https://github.com/jiangliuer326442/LogAnalysic](https://github.com/jiangliuer326442/LogAnalysic)

