
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

import com.inn.mpr.AvgPlayTimeByIdMapper;
import com.inn.rdcr.AvgReducer;

public class AvgPlayTimeById extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(AvgPlayTimeById.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(AvgPlayTimeById.class);
		job.setMapperClass(AvgPlayTimeByIdMapper.class);
		job.setCombinerClass(AvgReducer.class);
		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path("src/main/resources/VideoSession.dat"));
		FileOutputFormat.setOutputPath(job, new Path("src/main/output/"+this.getClass().getSimpleName()));
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		AvgPlayTimeById driver = new AvgPlayTimeById();
		int exitcode = ToolRunner.run(driver, args);
		System.exit(exitcode);
	}

}
