package com.freitas.hadoop.test;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;

import com.freitas.hadoop.BaseHadoopTest;


@Ignore
public class BaseModelRunTest extends BaseHadoopTest {
	
	/*
	 * This method is used when you know exactly how many lines are in your output
	 */
	protected List<String> getJobResults(FileSystem fs, Path outDir, int numLines) throws Exception {
		List<String> results = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(outDir);
		for (FileStatus file : fileStatus) {
			String name = file.getPath().getName();
			if (name.contains("part-r-00000")){
				Path filePath = new Path(outDir + "/" + name);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
				for (int i=0; i < numLines; i++){
					String line = reader.readLine();
					if (line == null){
						fail("Results are not what was expected");
					}
					results.add(line);
				}
				assertNull(reader.readLine());
				reader.close();
			}
		}
		return results;
	}
	
	/*
	 * This method is used when you have no idea how many lines there are, but you 
	 * want to get everything in the results and do additional comparison
	 */
	protected List<String> getJobResults(FileSystem fs, Path outDir) throws Exception {
		List<String> results = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(outDir);
		for (FileStatus file : fileStatus) {
			String name = file.getPath().getName();
			if (name.contains("part-r-00000")){
				Path filePath = new Path(outDir + "/" + name);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
				String line = null;
				while ((line = reader.readLine()) != null){
					results.add(line);
				}
				reader.close();
			}
		}
		return results;
	}

}
