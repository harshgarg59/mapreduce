package com.inn.mpr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class JsonMapper extends Mapper<Object, Text, Text, Text> {

	private static Logger log = Logger.getLogger(JsonMapper.class);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			JSONObject jobj = new JSONObject();
			String[] fields = value.toString().split(",");
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar startTime = Calendar.getInstance();
			startTime.setTime(df.parse(fields[4]));
			Calendar endTime = Calendar.getInstance();
			endTime.setTime(df.parse(fields[5]));
			long playtime = (df.parse(fields[5]).getTime() -df.parse(fields[4]).getTime()) / 1000;
			if (playtime > 0) {
				jobj.put("User ID", fields[0]);
				jobj.put("Video ID", fields[1]);
				jobj.put("Session ID", fields[2]);
				jobj.put("IP", fields[3]);
				jobj.put("Start Time", startTime.getTime());
				jobj.put("End Time", endTime.getTime());
				jobj.put("Player", fields[6]);
				jobj.put("Play Mode", fields[7]);
				log.info(jobj.toString());
				context.write(null, new Text(jobj.toString()));
			}
			log.info("JSONObject is " +jobj);
		} catch (Exception e) {
			log.error("Exception is " + e.getMessage());
		}
	}

}
