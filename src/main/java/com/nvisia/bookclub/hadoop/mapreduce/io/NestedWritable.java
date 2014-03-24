package com.nvisia.bookclub.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Writable implementation which allows any type of {@link Writable} instance to
 * be nested. This allows multiple {@link Writable} types to be used between
 * different mappers for multiple files.
 * 
 * NOTE: Does Hadoop have something like this already in the SDK?
 * 
 * @author Bryan Coller
 */
public class NestedWritable implements Writable {

	private Writable writable;
	
	public NestedWritable() {}
	
	public NestedWritable(Writable writable) {
		this.writable = writable;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		String className = dataInput.readUTF();
		if (className != null) {
			try {
				writable = (Writable) Class.forName(className).newInstance();
			} catch (Exception e) {
				// Print the stacktrace of what occured, the next line will
				// nullpointer the error stopping everything
				e.printStackTrace();
			}
			writable.readFields(dataInput);
		}
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(writable.getClass().getName());
		writable.write(dataOutput);
	}
	
	public Writable getWritable() {
		return writable;
	}

	public void setWritable(Writable writable) {
		this.writable = writable;
	}
}
