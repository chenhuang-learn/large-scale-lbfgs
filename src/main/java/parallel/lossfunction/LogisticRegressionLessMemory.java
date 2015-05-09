package parallel.lossfunction;

import java.net.URI;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parallel.avro.entry;
import parallel.avro.lbfgsdata;
import parallel.config.Config;
import parallel.util.FileOperator;

public class LogisticRegressionLessMemory implements LossFunction {
	
	//need : working_directory, num_map_tasks, num_reduce_tasks, num_variables
	private final Config config;
	
	public LogisticRegressionLessMemory(Config config) {
		this.config = config;
	}
	
	@Override
	public float evaluate(float[] w, float[] g, int k) throws Exception {
		Configuration conf = new Configuration();
		
		config.setMROptionalConfig(conf, "hadoop-conf.mr1.");
		
		String workingDirectory = config.workingDirectory;
		String jobName = config.jobName;
		String dataPath = config.dataPath;
		int dataMaxIndex = config.dataMaxIndex;
		int numReduceTasks = config.mr1NumReduceTasks;
		float l2_c = config.l2_c;
		
	    // save w to hdfs
		FileOperator.writeArrayOverwrite(new Path(workingDirectory, "w/w_" + k), conf, w);
		
		// setup a job, add w to distributed cache
		Job job = Job.getInstance(conf);
		job.setJarByClass(LogisticRegressionLessMemory.class);
		job.setJobName(jobName + "_mr1_" + k);
		
		job.getConfiguration().setInt("lbfgs_data_max_index", dataMaxIndex);
		job.getConfiguration().setFloat("lbfgs_l2_c", l2_c);
		
		FileInputFormat.setInputPaths(job, dataPath);
		Path outputPath = new Path(workingDirectory, "g/g_" + k);
		FileOperator.deleteFileIfExist(outputPath, job.getConfiguration());
		FileOutputFormat.setOutputPath(job, outputPath);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(LogisticRegressionLessMemoryMapper.class);
		AvroJob.setInputKeySchema(job, lbfgsdata.SCHEMA$);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setCombinerClass(LogisticRegressionLessMemoryCombiner.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(LogisticRegressionReducer.class);
		AvroJob.setOutputKeySchema(job, entry.SCHEMA$);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(numReduceTasks);
		job.addCacheFile(new URI((new Path(workingDirectory, "w/w_" + k).toString() + "#weightFile")));
		
		job.waitForCompletion(true);
		
		// read g from hdfs
		float loss = FileOperator.readGradientAndLoss(new Path(workingDirectory, "g/g_" + k), conf, g); 
		
		return loss;
	}
	
}
