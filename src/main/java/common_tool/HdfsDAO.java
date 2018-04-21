package common_tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDAO {

	public static void mkdirs(String folder, String hdfsPath, Configuration conf) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create: " + folder);
		}
		fs.close();
	}
	
	public static void rmr(String folder, String hdfsPath, Configuration conf) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.deleteOnExit(path);
		System.out.println("Delete: " + folder);
		fs.close();
	}
	
	public static void rename(String src, String dst, String hdfsPath, Configuration conf) throws IOException {
		Path name1 = new Path(src);
		Path name2 = new Path(dst);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.rename(name1, name2);
		System.out.println("Rename: from " + src + " to " + dst);
		fs.close();
	}
	
	public static void ls(String folder, String hdfsPath, Configuration conf) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out.println("==========================================================");
		for (FileStatus f : list) {
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}
	
	public static void createFile(String file, String content, String hdfsPath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + file);
		} finally {
			if (os != null)
				os.close();
		}
		fs.close();
	}
	
	public static void copyFile(String local, String remote, String hdfsPath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
		fs.close();
	}
	
	public static void download(String remote, String local, String hdfsPath, Configuration conf) throws IOException {
		Path path = new Path(remote);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyToLocalFile(path, new Path(local));
		System.out.println("download: from" + remote + " to " + local);
		fs.close();
	}
	
	public static String cat(String remoteFile, String hdfsPath, Configuration conf) throws IOException {
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + remoteFile);
		OutputStream baos = new ByteArrayOutputStream();
		String str = null;
		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, baos, 4096, false);
			str = baos.toString();
		} finally {
			IOUtils.closeStream(fsdis);
			fs.close();
		}
		System.out.println(str);
		return str;
	}
	
	/** 
	 * 读取指定路径下的文件  
	 * @param fileName 
	 */ 
	public static void read(String fileName, String hdfsPath, Configuration conf) throws IOException{  
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);  
        //read path  
        Path readPath=new Path(fileName);  
	    FSDataInputStream  inStream =null;  
	    try {  
	         inStream = fs.open(readPath);  
	                //read 输出到控制台System.out  
	         IOUtils.copyBytes(inStream, System.out, 4096,false);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }finally{  
            IOUtils.closeStream(inStream);  
        }  
    }
	
	/** 
     * 将本地文件 上传到HDFS 
     * @param sPath 本地文件地址 
     * @param dPath 目标文件地址 
     */  
	public static void write(String sPath,String dPath, String hdfsPath, Configuration conf) throws IOException{ 
        //目标文件  
        String putFileName=dPath;  
        Path writePath = new Path(putFileName);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataOutputStream outStream  = null;  
        FileInputStream inStream = null;  
        try {  
            //输出流  
            outStream= fs.create(writePath);  
              
            //输入流  
             inStream = new FileInputStream(new File(sPath));  
              
             //流操作   
             IOUtils.copyBytes(inStream, outStream, 4096,false);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }finally{  
            IOUtils.closeStream(inStream);  
            IOUtils.closeStream(outStream);  
        }  
    }
}
