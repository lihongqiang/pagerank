package org.apache.hadoop.pagerank;


import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.mapred.JobConf;

/**
 * rank文件与idlist文件分开
 * @author user-u1
 *
 */
public class PageRankJob2 {

    public static final String HDFS = "hdfs://192.168.1.32:9002";
//	public static final String HDFS = "hdfs://derc0:9009";
    public static final Pattern DELIMITER = Pattern.compile("[\t,:]");
    public static final Map<String, String> path = new HashMap<String, String>();
    
    public static void main(String[] args) {
        
    	Date t1,t2;
    	t1 = new Date();
        path.put("page", HDFS + "/user/user-u1/pagerank/forwardstatusnode.txt");// 数据文件			temp rank100 15
        path.put("input", HDFS + "/user/user-u1/pagerank/input");// HDFS的目录								node rank100 34
        path.put("output", HDFS + "/user/user-u1/pagerank/output");// 计算结果的PR
        path.put("sort", HDFS + "/user/user-u1/pagerank/sort");	//PR排序之后的结果
        path.put("lastSort", HDFS + "/user/user-u1/pagerank/lastSort");	//上一次排序的结果
        path.put("IDList", HDFS + "/user/user-u1/pagerank/IDList");//存储idlist的目录
        path.put("Pr", HDFS + "/user/user-u1/pagerank/Pr");//存储rank值的目录
        path.put("sortFile", HDFS + "/user/user-u1/pagerank/sort/part-r-00000");
        path.put("lastSortFile", HDFS + "/user/user-u1/pagerank/lastSort/part-r-00000" );    
        HdfsDAO hdfs = new HdfsDAO(HDFS, config());  
        try {
        	//初始化 id idlist			page ->  IDList
            InitialPageRankIDList.run(path);	
            
            //初始化 ip pr 			IDList  ->  Pr
            InitialPageRankPr.run(path);
           
//            //计算pr值					IDList + Pr  ->  output  变成    Pr
            PageRank4.run(path);				//分别取(di,idlist) 和(id,pr) 求新的(id,pr)          
    	             
            //根据pr排序				Pr  ->  sort
//            PageRankSort2.run(path);
//            
//            //sort转成lastSort		sort  变成       lastSort
//            hdfs.rmr(path.get("lastSort"));
//            hdfs.rename(path.get("sort"), path.get("lastSort"));
            
            int count = 0;
            Date start, end;
            start = new Date();
            while(true){
            	count ++ ;
            	System.out.println("count = " + count);
            	
//            	Date ss = new Date();
            	//计算pr值
            	PageRank4.run(path);			// IDList + Pr -> output	变成Pr			
            	
//            	Date ee = new Date();
//            	System.out.println("第一次迭代时间: " + (ee.getTime() - ss.getTime()));
            	
            	
                //根据pr排序						Pr  ->  sort
//                PageRankSort2.run(path);
                
//              判断是否结束循环，更新rank
//                if(hdfs.isOver3(path.get("sortFile"), path.get("lastSortFile"))){
//                	System.out.println("break ");
//                	break;
//                }else{
//                	hdfs.rmr(path.get("lastSort"));					//sort变成lastSort
//                	hdfs.rename(path.get("sort"), path.get("lastSort"));
//                }
                
            	if(count == 34) break;
            }
            end = new Date();
            t2 = new Date();
            System.out.println("平均迭代 时间 = " + (end.getTime() - start.getTime())/15);
            System.out.println("算法总运行时间 = " + (t2.getTime() - t1.getTime()));
            System.out.println("count = " + count);
            
//          //根据pr排序						Pr  ->  sort
            PageRankSort2.run(path);
            
        } catch (Exception e) {
        	System.out.println("error");
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(PageRankJob2.class);
        conf.setJobName("PageRank");
        return conf;
    }

    public static String scaleFloat(float f) {// 保留6位小数
        DecimalFormat df = new DecimalFormat("##0.000000");
        return df.format(f);
    }
    
    
}
