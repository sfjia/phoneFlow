package com.atguigu.shareFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/9/138:24
 */
public class SharedFriendMapper extends Mapper<LongWritable, Text,Text,Text> {
    /**
     * 好友列表：单向
     *  A:B,C,D,E,F,G
     *  B:X,C,D,E,V,H
     *
     */
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] str = line.split(":");
        String[] friends = str[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            v.set(str[0]);
            context.write(k,v);
        }
    }
}
