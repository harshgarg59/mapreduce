package com.inn.mapr.linec;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class LineCoutMapper extends Mapper<Object, Text, Text, LongWritable> {
	
	private static final Logger log = Logger.getLogger(LineCoutMapper.class);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		log.info("Mapper is started");
		String line = value.toString();
		if (line != null && !line.isEmpty()) {
			String[] split = line.split(" ");
			for(String s:split)
				context.write(new Text(s.trim()), new LongWritable(1));
		}
		log.info("Mapper is finished its task");
	}
}
