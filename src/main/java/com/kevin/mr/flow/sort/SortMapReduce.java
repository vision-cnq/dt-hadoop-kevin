package com.kevin.mr.flow.sort;
  
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 引入hadoop的自定义排序
 */
public class SortMapReduce {  
  
    public static class SortMapper extends  
            Mapper<LongWritable, Text, FlowBean, NullWritable> {  
        @Override  
        protected void map(  
                LongWritable k1,  
                Text v1,  
                Context context)
                throws IOException, InterruptedException {  
              
            String line = v1.toString();  
            String[] fields = StringUtils.split(line, "\t");  
  
            String phoneNB = fields[0];  
            long up_flow = Long.parseLong(fields[6]);
            long down_flow = Long.parseLong(fields[6]);
  
            context.write(new FlowBean(phoneNB, up_flow, down_flow),  
                    NullWritable.get());  
        }  
    }  
  
    public static class SortReducer extends  
            Reducer<FlowBean, NullWritable, Text, FlowBean> {  
        @Override  
        protected void reduce(FlowBean k2, Iterable<NullWritable> v2s,  
                Context context)
                throws IOException, InterruptedException {  
            String phoneNB = k2.getPhoneNB();  
            context.write(new Text(phoneNB), k2);  
        }  
    }  
  
    public static void main(String[] args) throws IOException,  
            ClassNotFoundException, InterruptedException {  
  
        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_01/1.txt",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);  
  
        job.setJarByClass(SortMapReduce.class);  
  
        job.setMapperClass(SortMapper.class);  
        job.setReducerClass(SortReducer.class);  
  
        job.setMapOutputKeyClass(FlowBean.class);  
        job.setMapOutputValueClass(NullWritable.class);  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(FlowBean.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
} 