package com.kevin.mr.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * @author kevin
 * @version 1.0
 * @description     二次排序
 * @createDate 2018/12/19
 */
public class GroupingMR {

    public static class GroupingMapper extends Mapper<LongWritable,Text,IntPair,IntWritable>{

        private final IntPair key = new IntPair();
        private final IntWritable value = new IntWritable();

        @Override
        protected void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(inValue.toString());
            int left = 0;
            int right = 0;
            if(itr.hasMoreTokens()){
                left = Integer.parseInt(itr.nextToken());
                if(itr.hasMoreTokens()){
                    right = Integer.parseInt(itr.nextToken());
                }
                key.set(left, right);
                value.set(right);
                context.write(key,value);

            }
        }
    }

    public static class GroupingReduce extends Reducer<IntPair,IntWritable,Text,IntWritable> {

        private static final Text SEPARATOR = new Text("-------");
        private final Text first = new Text();

        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            context.write(SEPARATOR,null);
            first.set(Integer.toString(key.getFirst()));
            for(IntWritable value: values) {
                context.write(first, value);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_06/",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);

        // 不使用Combiner，因为输出类型不一致
        job.setJarByClass(GroupingMR.class);
        job.setMapperClass(GroupingMapper.class);
        job.setReducerClass(GroupingReduce.class);

        // 使用分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // 输入的key，value类型
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 输出的key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
