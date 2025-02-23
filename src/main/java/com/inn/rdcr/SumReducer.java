package com.inn.rdcr;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SumReducer extends Reducer<Text, LongWritable, Text,  LongWritable> {
	private LongWritable result = new LongWritable();
	private static final Logger log = Logger.getLogger(SumReducer.class);

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
