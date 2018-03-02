package com.bcdata.analysis.yinni;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class OperatingFiles {

	// 文件系统连接到hdfs的配置信息
	private static Configuration getConf() {
		Configuration conf = new Configuration();
//		conf.set("mapred.job.tracker", "192.168.1.110:9001");
//		conf.set("fs.defaultFS", "hdfs://192.168.1.110:8020");

		return conf;
	}

	// 获取HDFS集群上所有节点信息
	public static void getDataNodeHost() throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
		}
		hdfs.close();
	}

	// delete the hdfs file, notice that the dst is the full path name
	public static boolean deleteHDFSFile(String dst) {
		boolean isDeleted = false;
		Configuration conf = getConf();
		try {
			FileSystem fs = FileSystem.get(new URI(dst), conf, "yuxuecheng");
			DistributedFileSystem hdfs = (DistributedFileSystem) fs;
			Path path = new Path(dst);
			if (hdfs.exists(path)) {
				isDeleted = hdfs.delete(path, true);
			}
			hdfs.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (InterruptedException ie) {
			// TODO Auto-generated catch block
			ie.printStackTrace();
		} catch (URISyntaxException urise) {
			// TODO Auto-generated catch block
			urise.printStackTrace();
		}

		return isDeleted;
	}

	//

	public static void main(String[] args) {
		try {
			getDataNodeHost();
			boolean result = deleteHDFSFile("example/result/");
			System.out.println(result);
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
			ioe.printStackTrace();
		}
	}
}
