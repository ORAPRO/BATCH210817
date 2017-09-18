package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//java com.laboros.HDFSService WordCount.txt /user/edureka -Ddfs.replication=1
public class HDFSService extends Configured implements Tool {
	
	public static void main(String[] args) {
		System.out.println("In Main Method");
		//step - 1 Validations
		if(args.length<2)
		{
			System.out.println("Java Usage: HDFSService /path/to/edgenode/local/file /path/to/hdfs/destination/directory");
			return;
		}
		
		//step-2 Load the configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		//step-3 Use ToolRunner.run
		try {
			int i= ToolRunner.run(conf, new HDFSService(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
		}
	}
	@Override
	public int run(String[] args) throws Exception 
	{
		//CREATING A FILE = CREATING METADATA + ADD DATA
		
		//STEP: 1 : CREATE METADATA
		System.out.println("In RUN METHOD");
		
		final String edgeNodeLocalFile = args[0];
		System.out.println("Local File Name:"+edgeNodeLocalFile);
		
		final String hdfsDestDir=args[1];
		System.out.println("HDFS Destination Directory:"+hdfsDestDir);
		
		final Path hdfsDestFilePath = new Path(hdfsDestDir, edgeNodeLocalFile);
		
		//Getting the configuration from the main method
		Configuration conf = getConf();
		
		FileSystem hdfs = FileSystem.get(conf);
		
		FSDataOutputStream fsdos=hdfs.create(hdfsDestFilePath);
		
		//ADD DATA
		
		InputStream is = new FileInputStream(edgeNodeLocalFile);
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}

}
