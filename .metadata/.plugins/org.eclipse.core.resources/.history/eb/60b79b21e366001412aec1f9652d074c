package storm.blueprints.chapter1.v1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AppendToBlob {
	//public static final String uri = "wasb://hanzstorm1@hanzstorage.blob.core.windows.net/aaa/a1.txt";
	public static final String uri = "hdfs://headnodehost:9000/a1.txt";

	public static void main(String[] args) throws Exception {
		// create byte array
		StringBuilder sb = new StringBuilder();
		for (int i = 10; i <= 15; i++) {
			sb.append("Data-");
			sb.append(i);
			sb.append("\n");
		}
		byte[] byt = sb.toString().getBytes();

		// check whether dfs.support.append is set to true
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(uri), conf);
		boolean isDfsSupportAppend = Boolean.getBoolean(hdfs.getConf().get("dfs.support.append"));
		System.out.println("dfs.support.append is set to be " + isDfsSupportAppend);
		isDfsSupportAppend=true;
		FSDataOutputStream fs = null;
		Path path= new Path(uri);
		if (isDfsSupportAppend) {
			fs = hdfs.append(path);
			System.out.println("append to "+ uri);
		} else {
			fs = hdfs.create(path);
			System.out.println("write to "+ uri);
		}
		fs.write(byt);
		fs.close();

		// read file
		BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(path)));
		String str = null;
		while ((str = bfr.readLine()) != null) {
			System.out.println(str);
		}
	}
}
