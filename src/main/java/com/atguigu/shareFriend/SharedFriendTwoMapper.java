package com.atguigu.shareFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/9/138:55
 */
public class SharedFriendTwoMapper extends Mapper<LongWritable, Text,Text,Text> {
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     *  好友： 用户，用户，用户，，，，
     *   B     A
     *   C     A    B
     */

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] str = line.split("\t");
        String friend = str[0];
        String person = str[1];
        String[] persons = person.split(",");

        //两两 求共同 好友
        for (int i = 0; i <persons.length ; i++) {

            for (int j = i+1; j <persons.length ; j++) {
                String togetherkey = persons[i] + "->" + persons[j];
                k.set(togetherkey);
                v.set(friend);
                context.write(k,v);
            }
        }
    }
}
