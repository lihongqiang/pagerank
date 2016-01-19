package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * ֻ��������������г��ȵĵ�
 * input id idlist
 * input id pr
 * output id pr
 * @author user-u1
 *
 */
public class PageRank4 {
	
	private static double d = 0.85;// ����ϵ��
	private static int num = 90675;	//�ڵ����

	//��һ��ת����ID PR,IDLIST��ʽ
	public static class PageRankMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		
		private String flag;// IDList or Pr
		private static int fileNum = 0;
		private static Map<String, String> mapIDList = new HashMap<>();
		private static Map<String, String> mapPr = new HashMap<>();

		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        FileSplit split = (FileSplit) context.getInputSplit();
	        flag = split.getPath().getParent().getName();// �ж϶������ݼ�
	        fileNum ++;
	        
//	        System.out.println("pagerank setup statrt");
	    }
			
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			System.out.println("key value");
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			if(flag.equals("IDList")){
				String id = itr.nextToken().toString();
				String list = itr.nextToken().toString();
//				System.out.println("id = "+ id + "  list = "+list);
				mapIDList.put(id, list);
				
				context.write(new Text(id), new DoubleWritable(0));	 			//������Щû����ȵĵ�
//				num ++;
			}else if(flag.equals("Pr")){
				String id = itr.nextToken().toString();
				String pr = itr.nextToken().toString();
//				System.out.println("id = " + id +"  " + "pr = " + pr);
				mapPr.put(id, pr);
				context.write(new Text(id), new DoubleWritable(0));	 			//������Щ���ȵĵ�
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
//			System.out.println("pagerank cleanup start");
//			System.out.println("fileNum = " + fileNum);
			// TODO Auto-generated method stub
			if(fileNum == 2){
				Iterator<Map.Entry<String, String>> iter = mapIDList.entrySet().iterator();
				while(iter.hasNext()){
					Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
					String id = entry.getKey();
					String value = entry.getValue();
					String [] list = PageRankJob2.DELIMITER.split(value);		//,�ָ�
					int sum = list.length;
					for(int i=0; i<list.length; i++){
						if(mapPr.containsKey(id)){								//�������������
							double npr = ( Double.parseDouble(mapPr.get(id)) / sum );
							context.write(new Text(list[i]), new DoubleWritable(npr));	//�´��ݸ���һ���ڵ��PRֵ
						}else{													//��������û����ȣ�ȡĬ��ֵ
//							System.out.println("id = " + id + " without Pr ");
						}
					}
				}
				fileNum = 0;
				mapIDList.clear();
				mapPr.clear();
			}
			super.cleanup(context);
		}
	}
	
	public static class PageRankReducer extends Reducer<Text, DoubleWritable, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double result = (1-d) ;
			double sum = 0 ;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			result = result + sum * d;
			
//			if(key.toString().equals("\"85839785\""))
//				System.out.println("id = 85839785" + " result =  " + result);
			
			context.write(key, new Text(""+result));
		}
		
		@Override
		protected void cleanup(Reducer<Text, DoubleWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("�ܵĽڵ����: " + num);
//			num = 0;								//num����
			super.cleanup(context);
		}
	}
		
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = PageRankJob2.config();
        
        String IDList = path.get("IDList");
        String Pr = path.get("Pr");
        String output = path.get("output");

        Job job = new Job(conf);
        job.setJarByClass(PageRank4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(IDList), new Path(Pr));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
        
        HdfsDAO hdfs = new HdfsDAO(PageRankJob2.HDFS, conf);  
//      //����Pr					output  ���     Pr
	    hdfs.rmr(path.get("Pr"));
	    hdfs.rename(path.get("output"), path.get("Pr"));
    }

}
