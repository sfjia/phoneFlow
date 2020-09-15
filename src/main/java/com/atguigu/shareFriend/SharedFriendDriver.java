package com.atguigu.shareFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;

/**
 * @Author 贾
 * @Date 2020/9/138:35
 *
 * 查询微博共同好友
 * 原始文件：
 *      A:B,C,D,E,G,F
 *      B:D,F,E,G,D,C
 *      C:S,Z,F,E,H
 *      ....
 * 第一次mapper，reduce之后
 *      好友： 用户，用户，用户，，，，
 *       B     A
 *       C     A    B
 *       ,,,
 *
 * 第二次mapper，reduce之后 两两好友之间的共同好友
 *
 *      A->B:  C,D,E,F,G
 *      A-C：  E,F
 *      B-C:   E,F
 *      ...
 */
public class SharedFriendDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        
        job.setJarByClass(SharedFriendDriver.class);
        job.setMapperClass(SharedFriendMapper.class);
        job.setReducerClass(SharedFriendReduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
