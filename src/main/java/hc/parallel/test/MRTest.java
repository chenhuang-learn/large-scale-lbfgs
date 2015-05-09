package hc.parallel.test;

import hc.parallel.avro.lbfgsdata;
import hc.parallel.config.Config;
import hc.parallel.util.FileOperator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRTest implements TestFunction {
	
	private final Config config;
	
	public MRTest(Config config) {
		this.config = config;
	}
	
	@Override
	public TestResult test(float[] x, float threshold, int k) throws Exception {
		Configuration conf = new Configuration();
		config.setMROptionalConfig(conf, "hadoop-conf.mr4.");
		
		String workingDirectory = config.workingDirectory;
		String jobName = config.jobName;
		String testDataPath = config.testDataPath;
		int dataMaxIndex = config.dataMaxIndex;
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRTest.class);
		job.setJobName(jobName + "_mr_test_" + k);
		
		job.getConfiguration().setInt("lbfgs_data_max_index", dataMaxIndex);
		job.getConfiguration().setFloat("lbfgs_test_threshold", threshold);
		
		FileInputFormat.setInputPaths(job, testDataPath);
		Path outputPath = new Path(workingDirectory, "t/t_" + k);
		FileOperator.deleteFileIfExist(outputPath, job.getConfiguration());
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(MRTestMapper.class);
		AvroJob.setInputKeySchema(job, lbfgsdata.SCHEMA$);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(MRTestReducer.class);
		job.setOutputKeyClass(TestResult.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(new Path(workingDirectory, "w/w_" + k).toString() + "#weightFile"));
		
		job.waitForCompletion(true);
		
		TestResult testResult = new TestResult();
		FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
		FileStatus[] status = fs.listStatus(outputPath);
		int outputFileNum = 0;
		FSDataInputStream streamin = null;
		for (int i = 0; i < status.length; i++) {
			if(status[i].isFile() && !FileOperator.isDiscardFile(status[i].getPath().getName())) {
				outputFileNum += 1;
				streamin = fs.open(status[i].getPath());
			}
		}
		if(outputFileNum != 1) {
			throw new IllegalStateException("illegal outputFileNum in mrtest : " + outputFileNum);
		}
		String result = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(streamin));
		String line = null;
		int lineNum = 0;
		while((line = br.readLine()) != null) {
			result += line;
			lineNum += 1;
		}
		if(lineNum != 1) {
			throw new IllegalStateException("illegal lineNum in mrtest : " + lineNum);
		}
		br.close();
		testResult.parseFromString(result);
		streamin.close();
		fs.close();
		
		return testResult;
	}

}
