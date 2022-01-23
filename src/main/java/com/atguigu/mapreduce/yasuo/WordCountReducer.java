package com.atguigu.mapreduce.yasuo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN ,reduce阶段输入的key的类型：Text
 * VALUEIN,reduce阶段输入的value的类型：IntWritable
 * KEYOUT,reduce阶段输出的key的类型：Text
 * VALUEOUT,reduce阶段输出的value的类型：IntWritable
 */
//继承Reducer，org.apache.hadoop.mapreduce
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //Iterable<IntWritable>类似以一个集合

        int sum = 0;
        //atguigu, (1,1)
        //1.累加
        for (IntWritable value : values) {
            sum += value.get();//sum是int类型，value是IntWritable类型，通过get方法转换
        }

        outV.set(sum);
        //2.写出
        context.write(key,outV);
    }
}
