package com.kevin.mr.flow;
  
import com.kevin.mr.flow.entity.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 这是job描述和提交类的规范写法
 * 手机流量统计
 */
public class FlowSumRunner extends Configured implements Tool {  
  
    public static void main(String[] args) throws Exception {  
        int res = ToolRunner  
                .run(new Configuration(), new FlowSumRunner(), args);  
        System.exit(res);  
    }  
  
    @Override  
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_01/1.txt",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);  
  
        job.setJarByClass(FlowSumRunner.class);  
  
        job.setMapperClass(FlowSumMapper.class);  
        job.setReducerClass(FlowSumReducer.class);  
  
        // job.setMapOutputKeyClass(Text.class);  
        // job.setMapOutputValueClass(FlowBean.class);  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(FlowBean.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());
  
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));
  
        // 执行成功，返回0，否则返回1  
        return job.waitForCompletion(true) ? 0 : 1;  
    }  
}  