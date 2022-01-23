package com.atguigu.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WebLogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"D:\\downloads\\hadoop-3.1.0\\data\\11_input\\inputlog",
                "D:\\downloads\\hadoop-3.1.0\\data\\output\\output14"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogDriver.class);
        job.setMapperClass(WebLogMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
