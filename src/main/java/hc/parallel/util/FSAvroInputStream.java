package hc.parallel.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FSDataInputStream;

public class FSAvroInputStream extends FSDataInputStream implements SeekableInput {

	public FSAvroInputStream(InputStream in) throws IOException {
		super(in);
	}

	@Override
	public long length() throws IOException {
		return this.length();
	}

	@Override
	public long tell() throws IOException {
		return this.getPos();
	}

}
