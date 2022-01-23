package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1、获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、关联本Driver的jar
        job.setJarByClass(FlowDriver.class);

        //3、关联Mapper和Reducer的jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4、设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5、设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(1);

        //6、设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\11_input\\inputflow"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\downloads\\hadoop-3.1.0\\data\\output\\output3"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
