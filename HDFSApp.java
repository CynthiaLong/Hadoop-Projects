package com.imooc.bigdata.hadoop_train_v2.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;



/*
 * Use Java API to operate HDFS file system 
 */
public class HDFSApp {
	
	public static final String HDFS_PATH = "hdfs://192.168.1.213:8020";
	FileSystem fileSystem = null;
	Configuration conf = null;
	
	@Before
	public void setUp() throws IOException, InterruptedException, URISyntaxException {
		System.out.println("----------SetUp----------");
		conf = new Configuration();
		conf.set("dfs.replication", "1");
//		conf.set("dfs.datanode.max.transfer.threads", "10000");
//		conf.set("dfs.datanode.socket.write.timeout", "300000");
//		conf.set("dfs.socket.timeout", "300000");
		fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "hadoop");
		
	}
	
	/*
	 * make a HDFS Directory
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path("/hdfsApi/test"));
	}
	
	/*
	 * check the HDFS file content
	 */
	@Test
	public void text() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
		IOUtils.copyBytes(in,System.out,1024);
	}
	
	/*
	 * create a new file in HDFS and write contents in it 
	 */
	@Test
	public void create() throws IllegalArgumentException, IOException {
		//FSDataOutputStream out = fileSystem.create(new Path("/hdfsApi/test/a.txt"));
		FSDataOutputStream out = fileSystem.create(new Path("/hdfsApi/test/b.txt"));
		out.writeChars("Hello Cynthia Long");
		out.flush();
		out.close();
	}
	
	/*
	 * check the replication parameter of the configuration
	 */
	@Test
	public void testConfiguration() {
		System.out.println(conf.get("dfs.replication"));
		System.out.println(conf.get("dfs.socket.timeout"));
		System.out.println(conf.get("dfs.datanode.socket.write.timeout"));
	}
	
	/*
	 * rename a file in HDFS
	 */
	@Test
	public void rename() throws IOException {
		Path oldPath = new Path("/hdfsApi/test/b.txt");
		Path newPath = new Path("/hdfsApi/test/c.txt");
		boolean result = fileSystem.rename(oldPath, newPath);
		System.out.println("rename outcome: "+result);
	}
	
	/*
	 * copy a local file to the HDFS
	 */
	@Test
	public void copyFromLocalFile() throws IOException {
		Path src = new Path("D:/Data Structure for Appl Eng/lab7/FindMedian.java");
		Path dst = new Path("/hdfsApi/test/FindMedian.java");
//		Path src = new Path("E:/Java/jdk-8u221-windows-x64.exe");
//		Path dst = new Path("/jdk-8u221-windows-x64.exe");
		fileSystem.copyFromLocalFile(src, dst);
	}
	
	/*
	 * copy a big local file to the HDFS
	 */
	@Test
	public void copyFromLocalBigFile() throws IllegalArgumentException, IOException {
		//success with file less than 5 M
		InputStream in = new BufferedInputStream(new FileInputStream(new File("D:/DatabaseManagement/DBslides.zip")));
		FSDataOutputStream out = fileSystem.create(new Path("/hdfsApi/test/DatabaseSlides.tgz"), 
		new Progressable() {
			public void progress() {
				System.out.print("*");
				}
		});
//		InputStream in = new BufferedInputStream(new FileInputStream(new File("E:/Java/jdk-8u221-windows-x64.exe")));
//		FSDataOutputStream out = fileSystem.create(new Path("/jdk-8u221-windows-x64.exe"), 
//		new Progressable() {
//			public void progress() {
//				System.out.print("*");
//				}
//		});
//		IOUtils.copyBytes(in, out, 4096);
	}
	
	/*
	 * copy a file from HDFS to local 
	 */
	@Test
	public void copyToLocalFile() throws IOException {
		Path src = new Path("/hdfsApi/test/FindMedian.java");
		Path dst = new Path("D:/Data Structure for Appl Eng");
		fileSystem.copyToLocalFile(false, src, dst, true);
	}
	
	/*
	 * list all the files under a directory, not recursively examine the subfolder
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		//not using listfiles(), which returns the statuses plus block locations of the files in the given path
		FileStatus[] files = fileSystem.listStatus(new Path("/"));  
		StringBuilder s;
		String isDir ;
	    String permission ;
	    short replication ;
		long length ;
		String path ;
		
		for(int i=0; i<files.length; i++) {
		    isDir = files[i].isDirectory() ? "folder": "file";
		    permission = files[i].getPermission().toString();
		    replication = files[i].getReplication();
			length = files[i].getLen();
			path = files[i].getPath().toString();
			s = new StringBuilder();
			s.append(isDir).append("\t").append(permission).append("\t").append(replication).append("\t").
			  append(length).append("\t").append(path);
			System.out.println(s);
		}
	}
	
    /*
     * list all the files under a directory, with recursively examining the subfolder
     */
	@Test
	public void listFilesRecursive() throws FileNotFoundException, IllegalArgumentException, IOException {
		//not listfiles(), which returns the statuses and block locations of the files in the given path
		RemoteIterator<LocatedFileStatus> files= fileSystem.listFiles(new Path("/hdfsApi"), true);  
		StringBuilder s; 
		String isDir ;
	    String permission ;
	    short replication ;
		long length ;
		String path ;
		LocatedFileStatus file ;
		
		while(files.hasNext()){
			file = files.next();
		    isDir = file.isDirectory() ? "folder": "file";
		    permission = file.getPermission().toString();
		    replication = file.getReplication();
			length = file.getLen();
			path = file.getPath().toString();
			s = new StringBuilder();
			s.append(isDir).append("\t").append(permission).append("\t").append(replication).append("\t").
			  append(length).append("\t").append(path);
			System.out.println(s);
		}
	}
	
	/*
	 * delete a file in HDFS
	 */
	@Test
	public void delete() throws IllegalArgumentException, IOException {
		boolean result = fileSystem.delete(new Path("/jdk-8u91-linux-x64.tar.gz"), true);
		if(result) {
			System.out.println("Successfully deleted!");
		}
		else {
			System.out.println("file not found");
		}
	}
	
	/*
	 * list the blocks infomation for a file in HDFS
	 */
	@Test
	public void getFileBlockLocations() throws IllegalArgumentException, IOException {
		FileStatus status = fileSystem.getFileStatus(new Path("/hadoop-2.6.0-cdh5.15.1.tar.gz"));
		BlockLocation[] blocks = fileSystem.getFileBlockLocations(status, 0, status.getLen());
		StringBuilder s ;
		for(BlockLocation loca: blocks) {
			s = new StringBuilder();
			//getNames returns the list of datanodes'names (IP:xferPort) hosting this block
			//because the replication param here is 1, we only have 1 name for each block
		    for(String name: loca.getNames()) {
		    	s.append(name).append("\t").append(loca.getOffset()).append(" : ").append(loca.getLength());
		    	System.out.println(s);
		    }
		}
	}
	
	@After
	public void tearDown() {
		conf = null;
		fileSystem = null;
		System.out.println("----------tearDown----------");
	}

//	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
//		URI uri = new URI("hdfs://192.168.0.213:8020");
//		Configuration conf = new Configuration();
//		FileSystem fileSystem = FileSystem.get(uri, conf, "hadoop");
//		System.out.println("success");
//		Path path = new Path("/hdfsApi/test");
//		boolean result;
//
//		HDFSApp app = new HDFSApp();
//		try {
//			app.copyFromLocalBigFile();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
}
