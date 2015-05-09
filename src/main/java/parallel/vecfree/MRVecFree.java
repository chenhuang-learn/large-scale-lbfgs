package parallel.vecfree;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import parallel.avro.entry;
import parallel.config.Config;
import parallel.util.FileOperator;

public class MRVecFree implements VecFreeFunction {

	private final Config config;
	
	public MRVecFree(Config config) {
		this.config = config;
	}
	
	public static String stringArrayToString(String[] fileNames) {
		if (fileNames.length == 0) {
			return "";
		} else {
			String result = fileNames[0];
			for(int i=1; i<fileNames.length; i++) {
				result += ("," + fileNames[i]);
			}
			return result;
		}
	}
	
	public static  String[] stringToStringArray(String s) {
		if(s.length() > 0) {
			String[] array = s.split(",");
			return array;
		} else {
			return new String[0];
		}
	}
	
	public static String fileNames2Paths(String[] old_s_file_names, String[] old_y_file_names, String s_file_name,
			String y_file_name, String pg_file_name, String workingDirectory) {
		String result = new Path(workingDirectory, "g/" + pg_file_name).toString();
		result += ("," + new Path(workingDirectory, "s/" + s_file_name).toString());
		result += ("," + new Path(workingDirectory, "y/" + y_file_name).toString());
		for (int i = 0; i < old_y_file_names.length; i++) {
			result += ("," + new Path(workingDirectory, "y/" + old_y_file_names[i]).toString());
		}
		for (int i = 0; i < old_s_file_names.length; i++) {
			result += ("," + new Path(workingDirectory, "s/" + old_s_file_names[i]).toString());
		}
		return result;
	}
	
	public static String fileNames2Paths(String s_y_file_names, String workingDirectory) {
		String[] s_y_file_name_array = stringToStringArray(s_y_file_names);
		String result = new Path(workingDirectory, "g/" + s_y_file_name_array[s_y_file_name_array.length-1]).toString();
		for (int i = 0; i < s_y_file_name_array.length - 1; i++) {
			if(i < (s_y_file_name_array.length - 1) / 2) {
				result += "," + new Path(workingDirectory, "s/" + s_y_file_name_array[i]).toString();
			} else {
				result += "," + new Path(workingDirectory, "y/" + s_y_file_name_array[i]).toString();
			}
		}
		return result;
	}
	
	public static Map<String, Integer> fileName2JobNum(String[] old_s_file_names, String[] old_y_file_names,
			String s_file_name, String y_file_name, String pg_file_name) {
		Map<String, Integer> m = new HashMap<String, Integer>();
		int index = 0;
		String[] new_file_names = {s_file_name, y_file_name, pg_file_name};
		for(int i = 0; i < new_file_names.length; i++) {
			for (int j = 0; j < old_s_file_names.length; j++) {
				m.put(new_file_names[i] + "," + old_s_file_names[j], index);
				index += 1;
			}
			for (int j = 0; j < old_y_file_names.length; j++) {
				m.put(new_file_names[i] + "," + old_y_file_names[j], index);
				index += 1;
			}
		}
		for(int i = 0; i < new_file_names.length; i++) {
			for(int j=0; j <= i; j++) {
				m.put(new_file_names[i] + "," + new_file_names[j], index);
				index += 1;
			}
		}
		return m;
	}
	
	public static boolean isValidNewb(Map<Integer, Float> newbMap, int reduceNum) {
		boolean result = true;
		if(newbMap.size() != reduceNum) {
			result = false;
		}
		for(Entry<Integer, Float> e : newbMap.entrySet()) {
			if(e.getKey() < 0 || e.getKey() >= reduceNum) {
				result = false;
			}
		}
		return result;
	}
	
	@Override
	public void vecsDot(float[][] b, int k, float[] x, float[] xp, float[] g,
			float[] gp, float[] pg) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		config.setMROptionalConfig(conf, "hadoop-conf.mr2.");
		String workingDirectory = config.workingDirectory;
		String jobName = config.jobName;
		
		// save s(k-1) and y(k-1)
		int m = config.param.m;
		int end = (k - 1) % m;
		String s_file_name = "s_" + end;
		String y_file_name = "y_" + end;
		String pg_file_name = "pg";
		FileOperator.writeArrayDiffOverwrite(new Path(workingDirectory, "s/" + s_file_name), conf, x, xp);
		FileOperator.writeArrayDiffOverwrite(new Path(workingDirectory, "y/" + y_file_name), conf, g, gp);
		FileOperator.writeArrayOverwrite(new Path(workingDirectory, "g/" + pg_file_name),  conf, pg);
		
		// when bound=1, b_new(effective_size)=3*3
		// when bound=m, b_new(effective_size)=(2m+1)*(2m+1)
		int real_m = k >= m ? m : k;
		if(k <= m) {
			// b from (2*(real_m-1)+1)^2 to (2*real_m+1)^2
			// map (2*(real_m-1))^2 matrix to b_new
			for(int i=2*(real_m-1)-1; i>=0; i--) {
				for(int j=2*(real_m-1)-1; j>=0; j--) {
					int new_i = (i < real_m-1) ? i : (i+1);
					int new_j = (j < real_m-1) ? j : (j+1);
					b[new_i][new_j] = b[i][j];
				}
			}
		} else {
			for(int i=1; i<2*real_m; i++) {
				for(int j=1; j<2*real_m; j++) {
					if(i==real_m || j==real_m) { continue; }
					int new_i = i-1;
					int new_j = j-1;
					b[new_i][new_j] = b[i][j];
				}
			}
		}
		
		// calculate new b for new s, y, pg
		String[] old_s_file_names = new String[real_m-1];
		String[] old_y_file_names = new String[real_m-1];
		if(real_m < m) {
			for(int i=0; i<end; i++) {
				old_s_file_names[i] = "s_" + i;
				old_y_file_names[i] = "y_" + i;
			}
		} else {
			int j = 0;
			for(int i=0; i<m; i++) {
				if(i != end) {
					old_s_file_names[j] = "s_" + i;
					old_y_file_names[j] = "y_" + i;
					j++;
				}
			}
		}
		conf.set("old_s_file_names", stringArrayToString(old_s_file_names));
		conf.set("old_y_file_names", stringArrayToString(old_y_file_names));
		conf.set("s_file_name", s_file_name);
		conf.set("y_file_name", y_file_name);
		conf.set("pg_file_name", pg_file_name);
		conf.setInt("real_m", real_m);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRVecFree.class);
		job.setJobName(jobName + "_mr2_" + k);
		
		FileInputFormat.setInputPaths(job, fileNames2Paths(old_s_file_names, old_y_file_names, s_file_name, y_file_name, pg_file_name, workingDirectory));;
		Path outputPath = new Path(workingDirectory, "b/new_b");
		FileOperator.deleteFileIfExist(outputPath, job.getConfiguration());
		FileOutputFormat.setOutputPath(job, outputPath);
		String keyValueSeparator = job.getConfiguration().get("mapreduce.output.textoutputformat.separator", "\t");
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(VecFreeMapper.class);
		AvroJob.setInputKeySchema(job, entry.SCHEMA$);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(VecFreeReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setNumReduceTasks(6 * real_m);
		job.setPartitionerClass(JobNumPartitioner.class);
		
		job.waitForCompletion(true);
		
		// read hadoop file, fill matrix b
		Map<Integer, Float> newbMap = FileOperator.readMR2Output(outputPath, job.getConfiguration(), keyValueSeparator);
		// valid newbMap?
		if(!isValidNewb(newbMap, 6*real_m)) {
			throw new IllegalStateException("invalid newb in mr2");
		}
		Map<String, Integer> f2jMap = fileName2JobNum(old_s_file_names, old_y_file_names, s_file_name, y_file_name, pg_file_name);
		int index_s_end = real_m - 1;
		int index_y_end = 2 * real_m - 1;
		int index_pg = 2 * real_m;
		for(int i=0; i<2*(real_m-1); i++) {
			int index_b = (i < real_m - 1) ? i : (i+1);
			String fileHead = (i < real_m - 1) ? "s_" : "y_";
			int index_s_y = (i < real_m - 1) ? i : (i-(real_m-1));
			index_s_y = end - (real_m - 1) + index_s_y;
			if(index_s_y < 0) {
				index_s_y = m + index_s_y;
			}
			b[index_s_end][index_b] = newbMap.get(f2jMap.get("s_" + end  + "," + fileHead + index_s_y));
			b[index_b][index_s_end] = b[index_s_end][index_b];
			b[index_y_end][index_b] = newbMap.get(f2jMap.get("y_" + end  + "," + fileHead + index_s_y));
			b[index_b][index_y_end] = b[index_y_end][index_b];
			b[index_pg][index_b] = newbMap.get(f2jMap.get("pg," + fileHead + index_s_y));
			b[index_b][index_pg] = b[index_pg][index_b];
		}
		b[index_s_end][index_s_end] = newbMap.get(f2jMap.get("s_" + end + "," + "s_" + end));
		b[index_y_end][index_y_end] = newbMap.get(f2jMap.get("y_" + end + "," + "y_" + end));
		b[index_pg][index_pg] = newbMap.get(f2jMap.get("pg,pg"));
		
		b[index_pg][index_s_end] = newbMap.get(f2jMap.get("pg," + "s_" + end));
		b[index_s_end][index_pg] = b[index_pg][index_s_end];
		b[index_pg][index_y_end] = newbMap.get(f2jMap.get("pg," + "y_" + end));
		b[index_y_end][index_pg] = b[index_pg][index_y_end];
		b[index_s_end][index_y_end] = newbMap.get(f2jMap.get("y_" + end + "," + "s_" + end));
		b[index_y_end][index_s_end] = b[index_s_end][index_y_end];
	}

	@Override
	public void computeDirect(float[] d, float[] theta, int k, float[] pg) throws IOException, ClassNotFoundException, InterruptedException {
		int m = config.param.m;
		int end = (k - 1) % m;
		int real_m = m <= k ? m : k;
		for (int i = 0; i < d.length; i++) {
			d[i] = 0.f;
		}
		
		Configuration conf = new Configuration();
		config.setMROptionalConfig(conf, "hadoop-conf.mr3.");
		conf.setInt("real_m", real_m);
		String s_y_file_names = "";
		for (int i = 0; i < 2 * real_m; i++) {
			String fileHead = i < real_m ? "s_" : "y_";
			int index_tmp = i < real_m ? i : i - real_m;
			index_tmp = end - (real_m - 1) + index_tmp;
			if(index_tmp < 0) {
				index_tmp = index_tmp + m;
			}
			if(i == 0) {
				s_y_file_names += fileHead + index_tmp;
			} else {
				s_y_file_names += "," + fileHead + index_tmp;
			}
			conf.setFloat("theta_" + i, theta[i]);
		}
		s_y_file_names += ",pg";
		conf.setFloat("theta_" + (2*real_m), theta[2*real_m]);
		conf.set("s_y_file_names", s_y_file_names);
		
		String jobName = config.jobName;
		String workingDirectory = config.workingDirectory;
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRVecFree.class);
		job.setJobName(jobName + "_mr3_" + k);
		
		FileInputFormat.setInputPaths(job, fileNames2Paths(s_y_file_names, workingDirectory));
		Path outputPath = new Path(workingDirectory, "d/new_d");
		FileOperator.deleteFileIfExist(outputPath, job.getConfiguration());
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(VecFreeMR3Mapper.class);
		AvroJob.setInputKeySchema(job, entry.SCHEMA$);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(VecFreeMR3Reducer.class);
		AvroJob.setOutputKeySchema(job, entry.SCHEMA$);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
		FileOperator.readArray(outputPath, job.getConfiguration(), d);
	}

}
