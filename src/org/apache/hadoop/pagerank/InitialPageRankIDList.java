package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 去重，相同的边只保留一次
 * input id id
 * output id idlist
 * @author user-u1
 *
 */
public class InitialPageRankIDList {
	
	public static class InitialMapper extends Mapper<Object, Text, Text, Text>{
		
//		private static Date startTime ;
//		private static Date endTime ;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
//			startTime = new Date();
		}
			
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String k = itr.nextToken();
				String v = itr.nextToken();
				if(k.substring(1, k.length()-1).equals("user_id") ){
					continue;
				}
				context.write(new Text(k), new Text(v));
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			endTime = new Date();
//			System.out.println("Time = " + (endTime.getTime() - startTime.getTime()));
			super.cleanup(context);
		}
	}
	
	public static class InitialReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Set s = new HashSet<String>();
			StringBuilder list = new StringBuilder();
			for(Text id : value){
				String id2 = id.toString();
				if(s.contains(id2)){						//去除重复的
					continue;
				}else{
					s.add(id2);
					list.append(id2 + ',');
				}
			}
			context.write(key, new Text(list.substring(0,list.length()-1).toString()));
		}
	}

	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = PageRankJob2.config();
        
        String page = path.get("page");
        String input = path.get("IDList");

        Job job = new Job(conf);
        job.setJarByClass(InitialPageRankIDList.class);
        job.setNumReduceTasks(1);  		//只有一个reducer

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(InitialMapper.class);
        job.setReducerClass(InitialReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(page));
        FileOutputFormat.setOutputPath(job, new Path(input));

        job.waitForCompletion(true);
    }

}
