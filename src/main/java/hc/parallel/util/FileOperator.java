package hc.parallel.util;

import hc.parallel.avro.entry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileOperator {
	
	private static String[] discardFiles = {"_SUCCESS"};
	
	public static boolean isDiscardFile(String fileName) {
		boolean result = false;
		for (int i = 0; i < discardFiles.length; i++) {
			if(fileName.equals(discardFiles[i])) {
				result = true;
			}
		}
		return result;
	}
	
	private static void readArray(FSDataInputStream streamin, float[] array) throws IOException {
		DatumReader<entry> datumReader = new SpecificDatumReader<entry>(entry.class);
		DataFileReader<entry> fileReader = new DataFileReader<entry>(new FSAvroInputStream(streamin), datumReader);
		entry e = null;
		while(fileReader.hasNext()) {
			e = fileReader.next(e);
			array[e.getIndex()] = e.getValue();
		}
		fileReader.close();
	}
	
	private static float readGradientAndLoss(FSDataInputStream streamin, float[] array) throws IOException {
		DatumReader<entry> datumReader = new SpecificDatumReader<entry>(entry.class);
		DataFileReader<entry> fileReader = new DataFileReader<entry>(new FSAvroInputStream(streamin), datumReader);
		float loss = (float) 0.0;
		entry e = null;
		while(fileReader.hasNext()) {
			e = fileReader.next(e);
			if(e.getIndex() == -1) {
				loss = e.getValue();
			} else {
				array[e.getIndex()] = e.getValue();
			}
		}
		fileReader.close();
		return loss;
	}
	
	private static void readArray(Path path, FileSystem fs, float[] array) throws IOException {
		FileStatus[] status = fs.listStatus(path);
		for(int i=0; i<status.length; i++) {
			if(status[i].isFile() && !isDiscardFile(status[i].getPath().getName())) {
				FSDataInputStream streamin = fs.open(status[i].getPath());
				readArray(streamin, array);
				streamin.close();
			}
		}
		fs.close();
	}
	
	public static float readGradientAndLoss(Path path, Configuration conf, float[] array) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] status = fs.listStatus(path);
		float loss = (float) 0.0;
		for(int i=0; i<status.length; i++) {
			if(status[i].isFile() && !isDiscardFile(status[i].getPath().getName())) {
				FSDataInputStream streamin = fs.open(status[i].getPath());
				float lossTemp = readGradientAndLoss(streamin, array);
				if(lossTemp != 0.0) {
					loss = lossTemp;
				}
				streamin.close();
			}
		}
		fs.close();
		return loss;
	}
	
	public static Map<Integer, Float> readMR2Output(Path path, Configuration conf, String separator) throws IOException {
		Map<Integer, Float> map = new HashMap<Integer, Float>();
		FileSystem fs = path.getFileSystem(conf);
		FileStatus[] status = fs.listStatus(path);
		for(int i=0; i<status.length; i++) {
			if(status[i].isFile() && !isDiscardFile(status[i].getPath().getName())) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()), "UTF-8"));
				String line = null;
				while((line = br.readLine()) != null) {
					line = line.trim();
					String[] fields = line.split(separator);
					map.put(Integer.parseInt(fields[0]), Float.parseFloat(fields[1]));
				}
				br.close();
			}
		}
		fs.close();
		return map;
	}
	
	public static void readArrayLocal(Path path, Configuration conf, float[] array) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		readArray(path, fs, array);
	}
	
	public static void readArray(Path path, Configuration conf, float[] array) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		readArray(path, fs, array);
	}
	
	public static void deleteFileIfExist(Path path, Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if(fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.close();
	}
	
	public static void writeArray(Path path, Configuration conf, float[] array) throws IOException {
		writeArray(path, conf, array, false);
	}
	
	public static void writeArrayOverwrite(Path path, Configuration conf, float[] array) throws IOException {
		writeArray(path, conf, array, true);
	}
	
	public static void writeArrayDiffOverwrite(Path path, Configuration conf, float[] array_1, float[] array_2) throws IOException {
		writeArrayDiff(path, conf, array_1, array_2, true);
	}
	
	private static void writeArray(Path path, Configuration conf, float[] array, boolean overwrite) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream streamout = fs.create(path, overwrite);
		writeArray(streamout, array);
		streamout.close();
		fs.close();
	}
	
	private static void writeArrayDiff(Path path, Configuration conf, float[] array_1, float[] array_2, boolean overwrite) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream streamout = fs.create(path, overwrite);
		writeArrayDiff(streamout, array_1, array_2);
		streamout.close();
		fs.close();
	}
	
	private static void writeArrayDiff(FSDataOutputStream streamout, float[] array_1, float[] array_2) throws IOException {
		if(array_1.length != array_2.length) {
			throw new IllegalStateException("array size error in writeArrayDiff");
		}
		DatumWriter<entry> datumWriter = new SpecificDatumWriter<entry>(entry.class);
		DataFileWriter<entry> fileWriter = new DataFileWriter<entry>(datumWriter);
		fileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
		fileWriter.create(entry.SCHEMA$, streamout);
		entry e = new entry();
		for (int i = 0; i < array_2.length; i++) {
			float diff = array_1[i] - array_2[i];
			if(diff != 0.f) {
				e.setIndex(i);
				e.setValue(diff);
				fileWriter.append(e);
			}
		}
		fileWriter.close();
	}
	
	private static void writeArray(FSDataOutputStream streamout, float[] array) throws IOException {
		DatumWriter<entry> datumWriter = new SpecificDatumWriter<entry>(entry.class);
		DataFileWriter<entry> fileWriter = new DataFileWriter<entry>(datumWriter);
		fileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
		fileWriter.create(entry.SCHEMA$, streamout);
		entry e = new entry();
		for(int i=0; i<array.length; i++) {
			if(array[i] != 0.0) {
				e.setIndex(i);
				e.setValue(array[i]);
				fileWriter.append(e);
			}
		}
		fileWriter.close();
	}
	
	public static void testArrayWR() throws IOException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.job.user.classpath.first", true);
		
		float[] array = new float[(int) 1e8];
		float[] old_array = new float[(int) 1e8];
		Random rand = new Random();
		for(int i=0; i<(int)5e7; i++) {
			array[i] = rand.nextFloat();
			old_array[i] = array[i];
		}
		writeArray(new Path("hc/array/array1"), conf, array);
		for(int i=0; i<array.length; i++) {
			if(i<(int)5e7) {
				array[i] = (float) 0.0;
			} else {
				array[i] = rand.nextFloat();
				old_array[i] = array[i];
			}
		}
		writeArray(new Path("hc/array/array2"), conf, array);
		readArray(new Path("hc/array"), conf, array);
		System.out.println(Arrays.equals(array, old_array));
	}
	
	public static void main(String[] args) throws IOException {
		float[] array = new float[Integer.parseInt(args[1])];
		readArrayLocal(new Path(args[0]), new Configuration(), array);
		for(int i=0; i<array.length; i++) {
			System.out.println(i + "\t" + array[i]);
		}
	}
	
}
