package parallel.vecfree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPair implements WritableComparable<IntPair> {
	private int jobNum;
	private int index;
	
	public IntPair() {}
	public IntPair(int jobNum, int index) {
		set(jobNum, index);
	}
	public void set(int jobNum, int index) {
		this.jobNum = jobNum;
		this.index = index;
	}
	public int getJobNum() {
		return jobNum;
	}
	public int getIndex() {
		return index;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		jobNum = in.readInt();
		index = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(jobNum);
		out.writeInt(index);
	}
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof IntPair))
			return false;
		if(jobNum == ((IntPair)o).jobNum && index == ((IntPair)o).index) {
			return true;
		} else {
			return false;
		}
	}
	@Override
	public int hashCode() {
		return jobNum * 163 + index;
	}
	@Override
	public int compareTo(IntPair o) {
		int cmp = (jobNum < o.jobNum ? -1 :(jobNum == o.jobNum ? 0 : 1));
		if(cmp != 0) {
			return cmp;
		}
		cmp = (index < o.index ? -1 : (index == o.index ? 0 : 1));
		return cmp;
	}
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntPair.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}
	static {
		WritableComparator.define(IntPair.class, new Comparator());
	}
}
