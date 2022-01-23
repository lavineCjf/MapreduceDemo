package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {//泛型为Reducer输出的泛型

    private  FSDataOutputStream atguiguOut;
    private  FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        //创建两条流
        //使用FileSystem（HDFS客户端创建流）
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());//与taskAttemptContext产生关联

            //crtl+alt+f,使之变为全局变量
            atguiguOut = fs.create(new Path("D:\\downloads\\hadoop-3.1.0\\data\\output\\Log1\\atguigu.log"));
            otherOut = fs.create(new Path("D:\\downloads\\hadoop-3.1.0\\data\\output\\Log1\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //抛出异常：
        //第一种：直接在方法抛出异常，交给上层处理
        //第二种：在本层生成try catch直接处理
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        //具体写
        String log = text.toString();
        if (log.contains("atguigu")){
            atguiguOut.writeBytes(log+"\n");
        }else{
            otherOut.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        //关流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
