package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN ,map阶段输入的key的类型：（偏移量）LongWritable
 * VALUEIN,map阶段输入的value的类型：（这一行的内容）Text
 * KEYOUT,map阶段输出的key的类型：（单词类型）Text
 * VALUEOUT,map阶段输出的value的类型：（单词次数）IntWritable
 */
//继承Mapper,Mapper类有两个
//(1)org.apache.hadoop.mapreduce对应2.x和3.x版本，mapreduce在2.x版本之后只负责计算
//(2)org.apache.hadoop.mapred对应1.x版本，mapred在1.x版本中既负责资源调度，又负责计算
//Text包应该是org.apache.hadoop.io

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //每一行数据都会进入map里面执行一次，一行一行去处理（若有7行则执行7次）

        //1.获取一行信息
        //atguigu atguigu
        //先把value（Text）类型转化为String类型，String类型有更多的操作（比如后续的切割）
        String line = value.toString();

        //2.切割
        String[] words = line.split(" ");
        //atguigu
        //atguigu

        //Text outK = new Text();    ①

        //3.循环写出
        for (String word : words) {
            //context.write(Text,IntWritable)
            //因此需要将String类型的word转化为Text
            // （Text outK = new Text();outK.set(word)，由于每一行也需要执行多次，每次创建一个对象就会浪费内存）
            //将它放在map里的话(①位置)，map执行几次，也会调用几次，同样浪费内存，因此放在最外层的类内，用private修饰
            //封装outK
            outK.set(word);

            //写出
            //对应的value为IntWritable类型，定义在最外层，值为1
            context.write(outK,outV);
            //context是连接map、reduce及系统代码进行交互的桥梁
        }
    }
}
