package com.freitas.hadoop.test;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import com.freitas.hadoop.BasicWordCount;

public class ModelRunTest extends BaseModelRunTest {
	
	@Test
	public void testBasicWordCount() throws Exception {
		
		String IN_DIR = "testing/wordcount/input";
		String OUT_DIR = "testing/wordcount/output";
		String DATA_FILE = "sample.txt";
		
		FileSystem fs = FileSystem.get(super.getConfiguration());
		Path inDir = new Path(IN_DIR);
		Path outDir = new Path(OUT_DIR);
		
		fs.delete(inDir, true);
		fs.delete(outDir,true);
		
		
		// create the input data files
		List<String> content = new ArrayList<String>();
		content.add("She sells seashells at the seashore, and she sells nuts in the mountain.");
		super.writeHDFSContent(fs, inDir, DATA_FILE, content);
		
		// set up the job, submit the job and wait for it complete
		Job job = new Job(conf, "mr test wordcount");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(BasicWordCount.TokenizerMapper.class);
		job.setReducerClass(BasicWordCount.IntSumReducer.class);
		FileInputFormat.addInputPath(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
		
		// now check that the output is as expected
		List<String> results = getJobResults(fs, outDir, 11);
		assertTrue(results.contains("She\t1"));
		assertTrue(results.contains("sells\t2"));
		
		// clean up after test case
		fs.delete(inDir, true);
		fs.delete(outDir,true);
	}
	
	
	
	

}
