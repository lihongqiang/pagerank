package org.apache.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import com.jcraft.jsch.Buffer;

public class HdfsDAO {

	public static final String HDFS = "hdfs://192.168.1.32:9002";
//	public static final String HDFS = "hdfs://derc0:9009";

    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    private String hdfsPath;
    private Configuration conf;

    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        String path = HDFS + "/user/user-u1/pagerank/input/part-r-00000";// HDFS的目录
        System.out.println(isOver(path));
    }

    public static JobConf config() {
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("HdfsDAO");
//        conf.addResource("classpath:core-site.xml");
//        conf.addResource("classpath:hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }
    
    public void cp(String src, String dst) throws IOException {
    	Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(name1, name2);
        System.out.println("copy: from " + src + " to " + dst);
        fs.close();
    }

    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    public void ls(String folder) throws IOException {
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

    public void createFile(String file, String content) throws IOException {
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

    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public String cat(String remoteFile) throws IOException {
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
     * input id pr_new:pr_old:idlist
     * 使用pr_new 与 pr_old来判断是否结束
     * @param path
     * @return
     */
    public static Boolean isOver(String path){
    	double eps = 1;
    	boolean result = true;
    	FileSystem hdfs;
    	InputStream in=null;
		try {
			hdfs = FileSystem.get(config());
			//使用缓冲流，进行按行读取的功能
	        BufferedReader buff=null;
	        //获取日志文件路径
	        Path file =new Path(path);
	        //打开文件流
	   	 	in=hdfs.open(file);
		   	//BufferedReader包装一个流
		   	buff=new BufferedReader(new InputStreamReader( in));	       	 
		   	String str;
		    while((str=buff.readLine())!=null){
		       	 String[] arr = PageRankJob2.DELIMITER.split(str);
		       	 double PR_new = Double.parseDouble(arr[1]);
		       	 double PR_old = Double.parseDouble(arr[2]);
		       	 if(Math.abs(PR_new - PR_old) > eps){
		       		 System.out.println(str);
		       		 System.out.println(Math.abs(PR_new - PR_old));
		       		 result = false;
		       		 break;
		       	 }
		    }
		    buff.close();
		    in.close();
		    hdfs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return result;
    }
    
    /**
     * 使用rank来判断是否结束,并修改rank
     * input id pr:rank:idlist
     * output id pr:rank:idlist
     * @param path
     * @return
     */
    public  Boolean isOver2(String path1, String path2){
    	boolean result = true;
    	FileSystem hdfs;
    	InputStream in=null;
    	FSDataOutputStream out=null;
		try {
			hdfs = FileSystem.get(config());
			//使用缓冲流，进行按行读取的功能
	        BufferedReader buff=null;
	        BufferedWriter wbuff=null;
	        //获取日志文件路径
	        Path file1 = new Path(path1);
	        Path file2 = new Path(path2);
	        //打开文件流
	   	 	in=hdfs.open(file1);
	   	 	out=hdfs.create(file2);
		   	//BufferedReader包装一个流
		   	buff=new BufferedReader(new InputStreamReader( in));
		   	
		   	String str;
		   	int num = 0;
		    while((str=buff.readLine())!=null){
		    	 num++;
		       	 String[] arr1 = str.split("\t");
		       	 String id = arr1[0];
		       	 String pridlist = arr1[1];
		       	 String[] arr2 = pridlist.split(":");
		       	 double pr = Double.parseDouble(arr2[0]);
		       	 int rank = Integer.parseInt(arr2[1]);
		       	 if(num != rank){
		       		 System.out.println("num = " + num + " " + "rank = "+rank + " id = "+id );
		       		 rank = num;
		       		if(num <= 100){
		       			result = false;
		       		}
		       	 }
		       	 String  idlist = "";
		       	 if(arr2.length > 2){
		       		idlist = arr2[2];
		       	 }
		       	 out.writeBytes(id + "\t" + pr + ":" + rank + ":" + idlist + "\n");
//		       	 out.writeUTF(id + "\t" + pr + ":" + rank + ":" + idlist + "\n");
		    }
		    buff.close();
		    in.close();
		    out.close();
		    hdfs.close();
		    
		    this.rmr(path1);
		    this.rename(path2, path1);
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	return result;
    }
    
    public  Boolean isOver3(String path1, String path2){
    	boolean result = true;
    	FileSystem hdfs;
    	InputStream in1=null;
    	InputStream in2=null;
    	List<String> list1 = new ArrayList<String>();
    	
		try {
			hdfs = FileSystem.get(config());
			//使用缓冲流，进行按行读取的功能
	        BufferedReader buff1=null;
	        BufferedReader buff2=null;
	        //获取文件路径
	        Path file1 = new Path(path1);
	        Path file2 = new Path(path2);
	        //打开文件流
	   	 	in1=hdfs.open(file1);
	   	 	in2=hdfs.open(file2);
	   	 	
		   	//BufferedReader包装一个流
		   	buff1=new BufferedReader(new InputStreamReader( in1));
		   	
		   	String str;
		    while((str=buff1.readLine())!=null){
		       	 String[] arr = str.split("\t");
		       	 String id = arr[0];
		       	 list1.add(id);
		    }
		    
		    //BufferedReader包装一个流
		    int num = 0;
		   	buff2=new BufferedReader(new InputStreamReader( in2));
		    while((str=buff2.readLine())!=null){
		    	 if(num == list1.size()){
		       		 break;
		       	 }
		    	 if(num == 100){
		    		 break;
		    	 }
		       	 String[] arr = str.split("\t");
		       	 String id = arr[0];
		       	 if(!list1.get(num).equals(id)){
		       		 System.out.println("num = " + (num+1) + " sort = " + list1.get(num) + " lastSort = " + id);
		       		 result = false;
		       		 break;
		       	 }
		       	 num ++ ;
		    }
		    buff1.close();
		    buff2.close();
		    in1.close();
		    in2.close();
		    hdfs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	return result;
    }
}