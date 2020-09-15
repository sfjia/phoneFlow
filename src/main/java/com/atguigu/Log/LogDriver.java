package com.atguigu.Log;

import com.atguigu.domain.FlowBean;
import com.atguigu.domain.FlowDriver;
import com.atguigu.domain.FlowMapper;
import com.atguigu.domain.FlowReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author 贾
 * @Date 2020/8/2719:06
 *
 *
 * 将一个本中的内容 包含atguigu的放在一个文件，不包含的放在另一个文件
 */
public class LogDriver {
    public static void main(String[] args) throws Exception {
        //1.获取job
        Configuration configuration = new Configuration();
        /**
         * 配置 map 端压缩
         */
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        /**
         * 配置 reduce 压缩
         */
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(LogDriver.class);
        // 关联mapper
        job.setMapperClass(LogMapper.class);
        //关联reduce
        job.setReducerClass(LogReduce.class);
        //关联map输出key,value
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        //关联最终输出keyValue
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputFormatClass(LogOuputFormat.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
