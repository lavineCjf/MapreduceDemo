package com.atguigu.mapreduce.yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class WordCountDriver {//mapreduce阶段若输出路径存在，则报错FileAlreadyExistsException
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job——> org.apache.hadoop.mapreduce
        Configuration conf = new Configuration();
//        conf.setBoolean("mapreduce.map.output.compress",true);
//        conf.setClass("mapreduce.map.output.compress.codec,", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);

        //2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3.关联mapper和reducer(jar包和mapper和reducer怎么产生联系？)
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径
        //并不是使用job进行设置，而是使用FileInputFormat和FileOutputFormat进行设置，并将job作为参数传入
        FileInputFormat.setInputPaths(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\11_input\\inputword"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\output\\output16"));

        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //7.提交job
        //waitForCompletion(传入参数true或false，传入为true时，monitorAndPrintJob启动，获得更多job的信息)
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
