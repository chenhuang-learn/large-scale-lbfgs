package hc.parallel.util;

import hc.parallel.avro.entry;
import hc.parallel.avro.lbfgsdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * @author chenhuang
 * provide two methods, libsvmFile->avroFile, avroFile->libsvmFile
 * feature index in libsvmFile can start from 0, label must be 1 or -1
 */
public class DataFormatTransform {
	
	public static void libSVMFile2AvroFile(String libSVMFile, String AvroFile)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				libSVMFile), "UTF-8"));
		String line = null;
		int lineNumber = 1;
		DatumWriter<lbfgsdata> datumWriter = new SpecificDatumWriter<lbfgsdata>(lbfgsdata.class);
		DataFileWriter<lbfgsdata> dataFileWriter = new DataFileWriter<lbfgsdata>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
		dataFileWriter.create(lbfgsdata.SCHEMA$, new File(AvroFile));
		lbfgsdata data = new lbfgsdata();
		while((line = br.readLine()) != null) {
			libSVMLine2LBFGSData(libSVMFile, lineNumber, line, data);
			lineNumber += 1;
			dataFileWriter.append(data);
		}
		dataFileWriter.close();
		br.close();
	}
	
	public static void AvroFile2LibSVMFile(String AvroFile, String libSVMFile) throws IOException {
		DatumReader<lbfgsdata> datumReader = new SpecificDatumReader<lbfgsdata>(lbfgsdata.class);
		DataFileReader<lbfgsdata> dataFileReader = new DataFileReader<lbfgsdata>(new File(AvroFile), datumReader);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(libSVMFile), "UTF-8"));
		while(dataFileReader.hasNext()) {
			bw.write(LBFGSData2LibSVMLine(dataFileReader.next()) + "\n");
		}
		bw.close();
		dataFileReader.close();
	}
	
	private static String LBFGSData2LibSVMLine(lbfgsdata data) {
		StringBuilder buffer = new StringBuilder();
		buffer.append(data.getResponse());
		for(entry e : data.getFeatures()) {
			buffer.append(" ");
			buffer.append(e.getIndex());
			buffer.append(":");
			buffer.append(e.getValue());
		}
		return buffer.toString();
	}
	
	private static void libSVMLine2LBFGSData(String fileName, int lineNumber, String line, lbfgsdata data) {
		try {
			if(data == null) {
				data = new lbfgsdata();
			}
			String[] fields = line.trim().split("\\s+");
			if(fields.length <= 1) {
				throw new RuntimeException("No label or No feature");
			}
			int label = Integer.parseInt(fields[0]);
			if((label != 1) && (label != -1)) {
				throw new RuntimeException("label Must be 1 or -1");
			}
			data.setResponse(label);
			List<entry> l = new ArrayList<entry>();
			int indexBefore = -1;
			for(int i=1; i<fields.length; i++) {
				String[] subFields = fields[i].split(":");
				int index = Integer.parseInt(subFields[0]);
				if(index <= indexBefore) {
					throw new RuntimeException("index <= indexBefore");
				}
				indexBefore = index;
				float value = Float.parseFloat(subFields[1]);
				l.add(new entry(index, value));
			}
			data.setFeatures(l);
			data.setWeight(1.f);
			data.setOffset(0.f);
		} catch(RuntimeException e) {
			throw new DataFormatException(fileName, lineNumber, e.getClass() + " " + e.getMessage());
		}
	}
	
	public static void testAvroAndLibSVM() throws IOException {
		libSVMFile2AvroFile("a9at.txt", "a9at.avro");
		AvroFile2LibSVMFile("a9at.avro", "a9at.new");
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length != 2) {
			System.out.println("java -jar libsvm2Avro.jar input output");
		}
		libSVMFile2AvroFile(args[0], args[1]);
	}
	
}
