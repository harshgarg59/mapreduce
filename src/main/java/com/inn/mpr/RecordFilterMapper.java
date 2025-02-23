package com.inn.mpr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class RecordFilterMapper extends Mapper<Object, Text, Text, LongWritable> {

	private static final Logger log = Logger.getLogger(RecordFilterMapper.class);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		log.info("Mapper is started");
		try {
			String line[] = value.toString().split(",");
			Date startTime = simpleDateFormat.parse(line[4]);
			Date endTime = simpleDateFormat.parse(line[5]);
			long time = (endTime.getTime() - startTime.getTime())/1000;
			log.info("playtime is "+time);
			if (time >0)
				context.write(new Text("Valid"), new LongWritable(time));
			else
				context.write(new Text("InValid"), new LongWritable(time));
		} catch (Exception e) {
			log.error("Exception  is {}" + e.getMessage());
		}
	}
}