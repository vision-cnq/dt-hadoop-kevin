package com.kevin.mr;

/**
 * 为了简化命令行方式运行作业，Hadoop自带了一些辅助类
 * 比较方便的方式是实现Tool接口，通过ToolRunner来运行应用程序
 * TooRunner内部调用GenericOperationsParser
 */
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 
public class Test1 extends Configured implements Tool {
	
	//枚举类型
	enum Counter 
	{
		LINESKIP,	//出错的行
	}
	
	/**  
	 * MAP任务
	 */  
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> 
	{
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
		{
			String line = value.toString();				//读取源数据
			
			try
			{
				//数据处理
				String [] lineSplit = line.split(" ");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
				Text out = new Text(month + ' ' + time + ' ' + mac);
				
				context.write( NullWritable.get(), out);	//输出 key \t value
			}
			catch ( ArrayIndexOutOfBoundsException e )
			{
				//调用getCounter()方法返回一个计数器，然后加1 ，由jobtracker维护
				context.getCounter(Counter.LINESKIP).increment(1);	//出错计数器+1
				return;
			}
		}
	}


	//run()方法为Configuration对象设定相应的值
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Test_1");								//任务名
		job.setJarByClass(Test1.class);								//指定class
		
		FileInputFormat.addInputPath( job, new Path(args[0]) );			//输入路径
		FileOutputFormat.setOutputPath( job, new Path(args[1]) );		//输出路径
		
		job.setMapperClass( Map.class );								//调用上面的Map类作为Map任务代码
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass( NullWritable.class );					//指定输出的Key的格式
		job.setOutputValueClass( Text.class );							//指定输出的Value的格式
		
		job.waitForCompletion(true);
		
		//输出任务完成情况
		System.out.println( "任务名称：" + job.getJobName() );
		System.out.println( "任务成功：" + ( job.isSuccessful()?"是":"否" ) );
		System.out.println( "输入行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() );
		System.out.println( "输出行数" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
		System.out.println( "跳过的行数：" + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

		return job.isSuccessful() ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception 
	{
		
		if ( args.length != 2 )
		{
			System.err.println("");
			System.err.println("Usage: Test_1 < input path > < output path > ");
			System.err.println("Example: hadoop jar ~/Test_1.jar hdfs://Master:9000/home/grid/Test_1 hdfs://Master:9000/home/grid/output1");
			System.err.println("Counter:");
			System.err.println("\t"+"LINESKIP"+"\t"+"Lines which are too short");
			System.exit(-1);
		}
		

		DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();
		
		//运行任务
		int res = ToolRunner.run(new Configuration(), new Test1(), args);


		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "开始时间：" + formatter.format(start) );
		System.out.println( "结束时间：" + formatter.format(end) );
		System.out.println( "执行耗时：" + String.valueOf( time ) + " 分钟" ); 

        System.exit(res);
	}
}