package com.imooc.bigdata.hadoop_train_v2.mr.wc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * the item with the same key arrives at the same reducer
	 */
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		Iterator<IntWritable>iter = arg1.iterator();
		int count = 0;
		while(iter.hasNext()) {
			IntWritable value = iter.next();
			count = count + value.get();
		}
		arg2.write(arg0, new IntWritable(count));
		System.out.println("reducing...");
	}

}
