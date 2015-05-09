package parallel.vecfree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import parallel.avro.entry;

public class VecFreeMapper extends Mapper<AvroKey<entry>, NullWritable, IntPair, FloatWritable>{
	
	private List<Integer> jobNum = null;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		int real_m = conf.getInt("real_m", -1);
		String s_file_name = conf.get("s_file_name");
		String y_file_name = conf.get("y_file_name");
		String pg_file_name = conf.get("pg_file_name");
		System.out.println(real_m + "," + s_file_name + "," + y_file_name + "," + pg_file_name);
		String[] old_s_file_names = MRVecFree.stringToStringArray(conf.get("old_s_file_names", ""));
		String[] old_y_file_names = MRVecFree.stringToStringArray(conf.get("old_y_file_names", ""));
		String file_name = ((FileSplit)context.getInputSplit()).getPath().getName();
		System.out.println(Arrays.toString(old_s_file_names) + "," + Arrays.toString(old_y_file_names) + "," + file_name);
		Map<String, Integer> map = MRVecFree.fileName2JobNum(old_s_file_names, old_y_file_names, s_file_name, y_file_name, pg_file_name);
		for(Entry<String, Integer> e : map.entrySet()) {
			System.out.println(e.getKey() + "," + e.getValue());
		}
		
		if(map.size() != (6*real_m)) {
			throw new IllegalStateException("error state in vecfree mapper(set up)");
		}
		jobNum = new ArrayList<Integer>();
		for(Entry<String, Integer> e : map.entrySet()) {
			String[] sa = e.getKey().split(",");
			for(int i=0; i<sa.length; i++) {
				if(file_name.equals(sa[i])) {
					jobNum.add(e.getValue());
				}
			}
		}
		System.out.println(Arrays.toString(jobNum.toArray()));
	}
	
	@Override
	public void map(AvroKey<entry> key, NullWritable value, Context context) throws IOException, InterruptedException {
		entry e = key.datum();
		IntPair pair = new IntPair();
		if(e.getValue() == 0.f) { return; }
		for(Integer num : jobNum) {
			pair.set(num, e.getIndex());
			context.write(pair, new FloatWritable(e.getValue()));
		}
	}
	
}
