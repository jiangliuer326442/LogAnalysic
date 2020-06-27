package com.mustafa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;

/**
 * 将log文件上传到hdfs文件系统
 * 示例：
 * gradle installDist 打成可执行文件
 * cd build/install/LogAnalysic 进入执行目录
 * bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_im_2020-06-23.log bigdata/jiazu/input/im/2020-06-23.log
 * bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_im_2020-06-24.log bigdata/jiazu/input/im/2020-06-24.log
 * bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_video_2020-06-23.log bigdata/jiazu/input/video/2020-06-23.log
 * bin/LogAnalysic com.mustafa.bigdata.Upload2Hdfs /home/mustafa/Documents/bigdata/jiazu/input/social_talklist_video_2020-06-24.log bigdata/jiazu/input/video/2020-06-24.log
 */
public class Upload2Hdfs {

    private static FileSystem getFileSystem() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem;
    }

    private static void write(String inpath, String outpath) throws Exception{
        FileSystem fileSystem = getFileSystem();
        //先删除再上传
        fileSystem.delete(new Path(outpath), true);

        FileInputStream in;
        FSDataOutputStream out;

        in = new FileInputStream(new File(inpath));
        out = fileSystem.create(new Path(outpath));
        IOUtils.copyBytes(in, out, 4096, false);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);

    }

    public static void main(String[] args) throws Exception {
        write(args[0], args[1]);
    }
}
