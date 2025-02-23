package com.inn.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.inn.mpr.JsonMapper;

public class JsonConvertor extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(JsonConvertor.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(JsonConvertor.class);
		job.setMapperClass(JsonMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("src/main/resources/VideoSession.dat"));
		FileOutputFormat.setOutputPath(job, new Path("src/main/output/"+this.getClass().getSimpleName()));
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		JsonConvertor driver = new JsonConvertor();
		int exitcode = ToolRunner.run(driver, args);
		System.exit(exitcode);
	}

}
