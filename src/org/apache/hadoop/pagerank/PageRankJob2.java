package org.apache.hadoop.pagerank;


import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.mapred.JobConf;

/**
 * rank�ļ���idlist�ļ��ֿ�
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
        path.put("page", HDFS + "/user/user-u1/pagerank/forwardstatusnode.txt");// �����ļ�			temp rank100 15
        path.put("input", HDFS + "/user/user-u1/pagerank/input");// HDFS��Ŀ¼								node rank100 34
        path.put("output", HDFS + "/user/user-u1/pagerank/output");// ��������PR
        path.put("sort", HDFS + "/user/user-u1/pagerank/sort");	//PR����֮��Ľ��
        path.put("lastSort", HDFS + "/user/user-u1/pagerank/lastSort");	//��һ������Ľ��
        path.put("IDList", HDFS + "/user/user-u1/pagerank/IDList");//�洢idlist��Ŀ¼
        path.put("Pr", HDFS + "/user/user-u1/pagerank/Pr");//�洢rankֵ��Ŀ¼
        path.put("sortFile", HDFS + "/user/user-u1/pagerank/sort/part-r-00000");
        path.put("lastSortFile", HDFS + "/user/user-u1/pagerank/lastSort/part-r-00000" );    
        HdfsDAO hdfs = new HdfsDAO(HDFS, config());  
        try {
        	//��ʼ�� id idlist			page ->  IDList
            InitialPageRankIDList.run(path);	
            
            //��ʼ�� ip pr 			IDList  ->  Pr
            InitialPageRankPr.run(path);
           
//            //����prֵ					IDList + Pr  ->  output  ���    Pr
            PageRank4.run(path);				//�ֱ�ȡ(di,idlist) ��(id,pr) ���µ�(id,pr)          
    	             
            //����pr����				Pr  ->  sort
//            PageRankSort2.run(path);
//            
//            //sortת��lastSort		sort  ���       lastSort
//            hdfs.rmr(path.get("lastSort"));
//            hdfs.rename(path.get("sort"), path.get("lastSort"));
            
            int count = 0;
            Date start, end;
            start = new Date();
            while(true){
            	count ++ ;
            	System.out.println("count = " + count);
            	
//            	Date ss = new Date();
            	//����prֵ
            	PageRank4.run(path);			// IDList + Pr -> output	���Pr			
            	
//            	Date ee = new Date();
//            	System.out.println("��һ�ε���ʱ��: " + (ee.getTime() - ss.getTime()));
            	
            	
                //����pr����						Pr  ->  sort
//                PageRankSort2.run(path);
                
//              �ж��Ƿ����ѭ��������rank
//                if(hdfs.isOver3(path.get("sortFile"), path.get("lastSortFile"))){
//                	System.out.println("break ");
//                	break;
//                }else{
//                	hdfs.rmr(path.get("lastSort"));					//sort���lastSort
//                	hdfs.rename(path.get("sort"), path.get("lastSort"));
//                }
                
            	if(count == 34) break;
            }
            end = new Date();
            t2 = new Date();
            System.out.println("ƽ������ ʱ�� = " + (end.getTime() - start.getTime())/15);
            System.out.println("�㷨������ʱ�� = " + (t2.getTime() - t1.getTime()));
            System.out.println("count = " + count);
            
//          //����pr����						Pr  ->  sort
            PageRankSort2.run(path);
            
        } catch (Exception e) {
        	System.out.println("error");
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop��Ⱥ��Զ��������Ϣ
        JobConf conf = new JobConf(PageRankJob2.class);
        conf.setJobName("PageRank");
        return conf;
    }

    public static String scaleFloat(float f) {// ����6λС��
        DecimalFormat df = new DecimalFormat("##0.000000");
        return df.format(f);
    }
    
    
}
