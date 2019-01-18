package com.kevin.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author kevin
 * @version 1.0
 * @description     倒排索引
 * @createDate 2018/12/19
 */
public class InverteIndex {

    // 将一行行的单词切分成一个个单词
    public static class InverteIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

        private static Text keyInfo = new Text(); //存储单词和URL组合
        private static final Text valueInfo = new Text("1");    //存储词频,初始化为1

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            // 得到字段数组
            String[] fields = StringUtils.split(line," ");
            // 得到这行数据所在的文件切片
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // 根据文件切片得到文件名
            String fileName = fileSplit.getPath().getName();

            for(String field : fields){
                // key值由单词和URL组成，如MapReduce:file1
                keyInfo.set(field + ":" + fileName);
                context.write(keyInfo, valueInfo);
            }

        }
    }

    //统计每个文件单词个数
    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{

        private static Text info = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int sum = 0;    // 统计词频
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            int splitIndex = key.toString().indexOf(":");
            // 重新设置value值由URL和词频组成
            info.set(key.toString().substring(splitIndex+1)+ ":" + sum);
            // 重新设置key值为单词
            key.set(key.toString().substring(0,splitIndex));

            context.write(key,info);
        }
    }

    // 将不同文件中相同的单词归类以分号分隔
    private  static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{

        private static Text result = new Text();
        // 输入：MapReduce file:2
        // 输出：MapReduce file1:1;file2:1;file3:2

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString()+ ";";
            }
            result.set(fileList);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_05/",
                "hdfs://master:9000/test/output/" };
        // 将配置传入Job，并启动任务
        Job job = Job.getInstance(conf);

        job.setJarByClass(InverteIndex.class);

        // 设置Mapper，Combiner，Reducer这些的类
        job.setMapperClass(InverteIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // 设置key，value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

        job.waitForCompletion(true);
    }

}
