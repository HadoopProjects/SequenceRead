package com.rahul.sequenceread;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileRead {

	public static void main(String[] args) throws IOException {
		Path path = new Path("hdfs://localhost:54310/user/hadoop/sequencefile");
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:54310/user/hadoop"), conf);
		
		SequenceFile.Reader reader = null;
		
		reader = new SequenceFile.Reader(fs, path, conf);
		Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		long position = reader.getPosition();
		
		while(reader.next(key, value)){
			String syncSeen = reader.syncSeen() ? "*" : "";
			System.out.print("[" + position + syncSeen + "]\t" + key + "\t" + value);
			position = reader.getPosition(); // beginning of next record
		}
		
		IOUtils.closeStream(reader);
	}

}
