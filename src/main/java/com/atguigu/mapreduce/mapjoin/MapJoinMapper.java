package com.atguigu.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable,Text, Text, NullWritable> {
    HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取缓存的文件，并把文件内容封装到集合pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());//先获取一个fs文件系统
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));//取出缓存文件中的值，这里只有一个
        //从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line=reader.readLine())){
            //切割
            String[] fields = line.split("\t");

            //赋值
            pdMap.put(fields[0],fields[1]);//pid,pname
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //处理order.txt:id,pid,amount
        String line = value.toString();
        String[] fields = line.split("\t");
        //获取pid所对应的pname
        String pname = pdMap.get(fields[1]);
        //获取id和amount
        //封装
        outK.set(fields[0]+"\t"+pname+"\t"+ fields[2]);
        context.write(outK,NullWritable.get());
    }
}
