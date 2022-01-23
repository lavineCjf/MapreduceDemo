package com.atguigu.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        //在Setup获取fileName，默认切片规则：一个文件一个切片。因此一个文件进入之后有一个setup方法，一个map方法
        //若不是用setup方法，则每一行都会获取当前文件的名称
        //fileName后续要使用，要设置为全局变量
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.判断是哪个文件
        if (fileName.contains("order")){//处理的是order表
            String[] split = line.split("\t");
            //3.封装kv
            //order表字段：id pid amount,key:pid,value:TableBean
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else{//处理的是pd表
            String[] split = line.split("\t");
            //3.封装kv
            //pd表字段：pid pname,key:pid,value:TableBean
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        //写出
        context.write(outK,outV);
    }
}
