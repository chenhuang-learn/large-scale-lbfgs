package hc.parallel.test;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class TestResult implements WritableComparable<TestResult> {
	
	public int truePositive = 0;
	public int falsePositive = 0;
	public int trueNegative = 0;
	public int falseNegative = 0;
	public float accuracy = 0.f;
	public float precision = 0.f;
	public float recall = 0.f;
	public float f1_score = 0.f;
	public float auc = 0.f;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		truePositive = in.readInt();
		falsePositive = in.readInt();
		trueNegative = in.readInt();
		falseNegative = in.readInt();
		accuracy = in.readFloat();
		precision = in.readFloat();
		recall = in.readFloat();
		f1_score = in.readFloat();
		auc = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(truePositive);
		out.writeInt(falsePositive);
		out.writeInt(trueNegative);
		out.writeInt(falseNegative);
		out.writeFloat(accuracy);
		out.writeFloat(precision);
		out.writeFloat(recall);
		out.writeFloat(f1_score);
		out.writeFloat(auc);
	}

	@Override
	public int compareTo(TestResult arg0) {
		return 0;
	}
	
	@Override
	public String toString() {
		return truePositive + "," + falsePositive + "," + trueNegative + "," + falseNegative + "," + accuracy
				+ "," + precision + "," + recall + "," + f1_score + "," + auc;
	}
	
	public String toPrintString() {
		return "\ttruePositive=" + truePositive + " falsePositive=" + falsePositive + " trueNegative=" + trueNegative + " falseNegative=" + falseNegative + " \n"
				+ "\taccuracy=" + accuracy + " precision=" + precision + " recall=" + recall + " f1_score=" + f1_score + " \n"
				+ "\tauc=" + auc + " ";
	}
	
	public void parseFromString(String str) {
		String[] results = str.split(",");
		if(results.length != 9) {
			throw new IllegalStateException("error test results length :" + results.length);
		}
		truePositive = Integer.parseInt(results[0]);
		falsePositive = Integer.parseInt(results[1]);
		trueNegative = Integer.parseInt(results[2]);
		falseNegative = Integer.parseInt(results[3]);
		accuracy = Float.parseFloat(results[4]);
		precision = Float.parseFloat(results[5]);
		recall = Float.parseFloat(results[6]);
		f1_score = Float.parseFloat(results[7]);
		auc = Float.parseFloat(results[8]);
	}
	
	public TestResult calTestResult(float[] positiveProbs, float[] negativeProbs, float threshold) {
		for(int i=0; i<positiveProbs.length; i++) {
			if(positiveProbs[i] >= threshold) {
				truePositive += 1;
			} else {
				falseNegative += 1;
			}
		}
		for(int i=0; i<negativeProbs.length; i++) {
			if(negativeProbs[i] >= threshold) {
				falsePositive += 1;
			} else {
				trueNegative += 1;
			}
		}
		int numPredictPositive = truePositive + falsePositive;
		int numPredictNegative = trueNegative + falseNegative;
		accuracy = ((float)(truePositive + trueNegative)) / (numPredictNegative + numPredictPositive);
		precision = ((float)truePositive) / numPredictPositive;
		recall = ((float)truePositive) / positiveProbs.length;
		f1_score = 2 / (1/precision + 1/recall);
		auc = calAUC(positiveProbs, negativeProbs);
		return this;
	}
	
	public float calAUC(float[] positiveProbs, float[] negativeProbs) {
		Arrays.sort(positiveProbs);
		Arrays.sort(negativeProbs);
		int n0 = negativeProbs.length;
		int n1 = positiveProbs.length;
		if(n0 == 0 || n1 == 0) {
			return 0.5f;
		}
		//sacn the data
		int i0 = 0, i1 = 0, rank = 1;
		double rankSum = 0;
		while(i0 < n0 && i1 < n1) {
			float v0 = negativeProbs[i0];
			float v1 = positiveProbs[i1];
			if(v0 < v1) {
				i0++;
				rank++;
			} else if(v1 < v0) {
				i1++;
				rankSum += rank;
				rank++;
			} else {
				float tieScore = v0;
				// how many negatives are tied?
				int k0 = 0;
				while(i0 < n0 && negativeProbs[i0] == tieScore) {
					k0++;
					i0++;
				}
				// how many positives are tied?
				int k1 = 0;
				while(i1 < n1 && positiveProbs[i1] == tieScore) {
					k1++;
					i1++;
				}
				// we found k0+k1 tied values ranks in [rank, rank+k0+k1)
				rankSum += (rank + (k0 + k1 - 1) / 2.0) * k1;
				rank += k0 + k1;
			}
		}
		if(i1 < n1) {
			rankSum += (rank + (n1 - i1 - 1) / 2.0) * (n1 - i1);
			rank += n1 - i1;
		}
		return (float) ((rankSum / n1 - (n1 + 1)/ 2.0) / n0);
	}
	
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0]))));
		String line = br.readLine();
		List<Float> neg_list = new ArrayList<Float>();
		List<Float> pos_list = new ArrayList<Float>();
		while ((line = br.readLine()) != null) {
			String[] fields = line.split(" ");
			if(fields[0].equals("1")) {
				pos_list.add(Float.parseFloat(fields[1]));
			} else {
				neg_list.add(Float.parseFloat(fields[1]));
			}
		}
		float[] pos_array = new float[pos_list.size()];
		float[] neg_array = new float[neg_list.size()];
		for(int i=0; i<pos_list.size(); i++) {
			pos_array[i] = pos_list.get(i);
		}
		for(int i=0; i<neg_list.size(); i++) {
			neg_array[i] = neg_list.get(i);
		}
		System.out.println(new TestResult().calAUC(pos_array, neg_array));
		br.close();
	}
	
}
