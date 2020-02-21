package com.imooc.bigdata.hadoop_train_v2.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * KEYIN, the key of read in data, offset of each row 
 * VALUEIN, the value of read in data
 * KEYOUT, the self defined key of output data
 * VALUEOUT, the self defined value of output data
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] words = value.toString().split("\t");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
		System.out.println("mapping...");		
	}
	
}
