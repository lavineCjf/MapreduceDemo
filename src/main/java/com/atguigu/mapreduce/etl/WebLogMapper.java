package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();

        //2.解析日志（字段大于11的保留，小于等于11的删除）
        boolean result = parseLog(line,context);

        //3.日志不合法退出
        if (!result){
            return;
        }

        //4.日志合法就直接写出
        context.write(value,NullWritable.get());
    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        //1.截取
        String[] fields = line.split(" ");//按空格截取

        //2.日志长度大于11的为合法
        if (fields.length>11){
            return  true;
        }else {
            return false;
        }
    }
}
