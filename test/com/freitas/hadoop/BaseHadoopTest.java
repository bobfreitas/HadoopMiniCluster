package com.freitas.hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * This class is used to create a Mini Cluster.  It is essentially a knock off of the 
 * standard Hadoop class ClusterMapReduceTestCase with the change that cluster/config are 
 * accessible to child classes, the configuration uses fixed setting to give the cluster 
 * an identity and the starting and stopping is done once in the BeforeClass and AfterClass, 
 * which means the cluster only gets started once, as opposed to being started 
 * and stopped for each test.  
 */
@Ignore
@SuppressWarnings({ "rawtypes", "deprecation" })
public abstract class BaseHadoopTest {

	protected static MiniDFSCluster dfsCluster = null;
	protected static MiniMRCluster mrCluster = null;
	protected static JobConf conf = null;
	private static final int NAMENODE_PORT = 9010;
	private static final int JOBTRACKER_PORT = 9011;
	protected static String LOG_DIR = "/tmp/logs";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// make sure the log dir exists
		File logPath = new File(LOG_DIR);
		if (!logPath.exists()){
			logPath.mkdirs();
		}
		// configure and start the cluster
		System.setProperty("hadoop.log.dir", LOG_DIR);
		System.setProperty("javax.xml.parsers.SAXParserFactory",
				"com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
		Properties props = new Properties();
		props.setProperty("dfs.datanode.data.dir.perm", "775");
		conf = new JobConf();
		String hadoopConfDir = "test" + File.separator + "resources" + File.separator + "hadoop" + File.separator + "conf";
		String coreSitePath = hadoopConfDir + File.separator + "core-site.xml";
		conf.addResource(new Path(coreSitePath));
		String hdfsSitePath = hadoopConfDir + File.separator + "hdfs-site.xml";
		conf.addResource(new Path(hdfsSitePath));
		String mrSitePath = hadoopConfDir + File.separator + "mapred-site.xml";
		conf.addResource(new Path(mrSitePath));
		startCluster(true, conf, props);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		stopCluster();
		// clean up the hdfs files created by mini cluster
		String baseTempDir = "build" + File.separator + "test" + File.separator;
		String dfsDir = baseTempDir + "data";
		FileUtils.deleteDirectory(new File(dfsDir));
		String mrDir = baseTempDir + "mapred";
		FileUtils.deleteDirectory(new File(mrDir));
		FileUtils.cleanDirectory(new File(LOG_DIR));
	}

	protected static synchronized void startCluster(boolean reformatDFS, JobConf conf ,Properties props)
			throws Exception {
		if (dfsCluster == null) {
			
			if (props != null) {
				for (Map.Entry entry : props.entrySet()) {
					conf.set((String) entry.getKey(), (String) entry.getValue());
				}
			}
			dfsCluster = new  MiniDFSCluster(NAMENODE_PORT, conf, 2, reformatDFS, true, null, null);
			ConfigurableMiniMRCluster.setConfiguration(props);
			mrCluster = new ConfigurableMiniMRCluster(2, dfsCluster.getFileSystem().getName(),
					1, conf);
		}
	}
	
	protected static void stopCluster() throws Exception {
		if (mrCluster != null) {
			mrCluster.shutdown();
			mrCluster = null;
		}
		if (dfsCluster != null) {
			dfsCluster.shutdown();
			dfsCluster = null;
		}
	}
	
	protected FileSystem getFileSystem() throws IOException {
		return dfsCluster.getFileSystem();
	}

	protected MiniMRCluster getMRCluster() {
		return mrCluster;
	}
	
	protected Configuration getConfiguration() {
		return conf;
	}

	protected Path getTestRootDir() {
		return new Path("x").getParent();
	}

	protected Path getInputDir() {
		return new Path("input");
	}

	protected Path getOutputDir() {
		return new Path("output");
	}

	protected JobConf createJobConf() {
		return mrCluster.createJobConf();
	}
	
	private static class ConfigurableMiniMRCluster extends MiniMRCluster {
		private static Properties config;

		public static void setConfiguration(Properties props) {
			config = props;
		}

		public ConfigurableMiniMRCluster(int numTaskTrackers, String namenode,
				int numDir, JobConf conf)
						throws Exception {
			super(JOBTRACKER_PORT,0, numTaskTrackers, namenode, numDir, null, null, null, conf);
		}

		public JobConf createJobConf() {
			JobConf conf = super.createJobConf();
			if (config != null) {
				for (Map.Entry entry : config.entrySet()) {
					conf.set((String) entry.getKey(), (String) entry.getValue());
				}
			}
			return conf;
		}
	}
	
	protected void writeHDFSContent(FileSystem fs, Path dir, String fileName, List<String> content) throws IOException {
		FSDataOutputStream out = fs.create(new Path(dir, fileName));
		for (String line : content){
			out.writeBytes(line);
		}
		out.close();
	}
	
	protected String createTempLocalFile(String fileName, String fileExt, List<String> content) throws Exception {
		File tmpFile = File.createTempFile( fileName, fileExt);
		tmpFile.deleteOnExit();
		BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));
		for (String line : content){
			bw.write(line);
		}
		bw.close();
		return tmpFile.getAbsolutePath();
	}
	
}
