package org.apache.hadoop.pagerank;

import java.io.IOException;
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
 * 
 * input id idList
 * output id pr
 * @author user-u1
 *
 */
public class InitialPageRankPr {
	
	public static class InitialMapper extends Mapper<Object, Text, Text, Text>{
			
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String k = itr.nextToken();				//id
				String v = itr.nextToken();				//idList
				context.write(new Text(k), new Text("1"));//id pr
			}
		}
	}
	
	public static class InitialReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, new Text("1"));
		}
	}

	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = PageRankJob2.config();
        
        String page = path.get("IDList");
        String input = path.get("Pr");

        Job job = new Job(conf);
        job.setJarByClass(InitialPageRankPr.class);
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
