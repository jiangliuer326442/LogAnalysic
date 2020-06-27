package com.mustafa.bigdata;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * 将log文件解析为csv
 * 示例：
 * gradle jar
 * HADOOP_HOME=/data/home/software/hadoop-2.6.0-cdh5.16.2
$HADOOP_HOME/bin/yarn jar \
build/libs/LogAnalysic-1.0-SNAPSHOT.jar \
com.mustafa.bigdata.ParseLog2Csv bigdata/jiazu/input/im bigdata/jiazu/output/im

$HADOOP_HOME/bin/yarn jar \
build/libs/LogAnalysic-1.0-SNAPSHOT.jar \
com.mustafa.bigdata.ParseLog2Csv bigdata/jiazu/input/video bigdata/jiazu/output/video
 */
public class ParseLog2Csv extends Configured implements Tool {

    public static class Log2CsvMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.getLength() > 0){
                String rowContent = value.toString();
                String _time = rowContent.split(",")[0];

                String jsonString = StringUtils.join(Arrays.copyOfRange(rowContent.split(","), 2, 4), ",");
                JsonArray jsonArray = new JsonParser().parse(jsonString).getAsJsonArray();

                String fromUid = jsonArray.get(0).toString().replaceAll("\"", "");
                String toUid = jsonArray.get(1).toString().replaceAll("\"", "");

                context.write(key, new Text(fromUid + "\t" + toUid + "\t" + _time));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(Log2CsvMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new ParseLog2Csv(), args));
    }
}
