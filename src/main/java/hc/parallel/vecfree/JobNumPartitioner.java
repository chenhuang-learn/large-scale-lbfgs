package hc.parallel.vecfree;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class JobNumPartitioner extends Partitioner<IntPair, FloatWritable>{
	@Override
	public int getPartition(IntPair key, FloatWritable value, int numPartitions) {
		return key.getJobNum();
	}
}
