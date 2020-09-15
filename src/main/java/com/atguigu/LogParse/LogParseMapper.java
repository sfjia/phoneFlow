package com.atguigu.LogParse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2917:50
 */
public class LogParseMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Boolean flag =  LogParse(line,context);
        if(!flag){
            return;
        }
        k.set(line);
        context.write(k,NullWritable.get());
    }

    private Boolean LogParse(String line, Context context) {
        String[] words = line.split(" ");
        if(words.length>11){
            context.getCounter("map","true").increment(1);
            return true;
        }
        context.getCounter("map","false").increment(1);
        return false;
    }
}
