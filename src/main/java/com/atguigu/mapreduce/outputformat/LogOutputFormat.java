package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//先定义一个类继承FileOutputFormat
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    //重写getRecordWritable方法，返回RecordWritable，没有就创建一个
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        LogRecordWriter lrw = new LogRecordWriter(taskAttemptContext);//与taskAttemptContext连接
        return lrw;
    }
}
