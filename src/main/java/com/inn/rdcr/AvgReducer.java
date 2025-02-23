package com.inn.rdcr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AvgReducer extends Reducer<Text, LongWritable, Text,  LongWritable> {
	private LongWritable result = new LongWritable();
	private static final Logger log = Logger.getLogger(AvgReducer.class);

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int count=0;
		for (LongWritable val : values) {
			sum += val.get();
			count++;
		}
		log.info("Sum is "+sum);
		log.info("count  is "+count);
		result.set(sum/count);
		context.write(key, result);
	}
}
