package com.inn.mpr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AvgPlayTimeMapper extends Mapper<Object, Text, Text, LongWritable> {

	private static final Logger log = Logger.getLogger(AvgPlayTimeMapper.class);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		log.info("Mapper is started");
		try {
			String line[] = value.toString().split(",");
			Date startTime = simpleDateFormat.parse(line[4]);
			Date endTime = simpleDateFormat.parse(line[5]);
			long time = (endTime.getTime() - startTime.getTime())/1000;
			if (time >0)
				context.write(new Text("sec"), new LongWritable(time));
			log.info("playtime is "+time);
		} catch (Exception e) {
			log.error("Exception  is {}" + e.getMessage());
		}
	}
}