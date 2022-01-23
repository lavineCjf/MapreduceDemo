package com.atguigu.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //注意URI的输入格式
        job.addCacheFile(new URI("file:///D:/downloads/hadoop-3.1.0/data/11_input/tablecache/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\11_input\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\output\\output13"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
