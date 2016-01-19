package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.pagerank.MyDoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * input	id pr
 * output	id pr(按pr大小排序)
 * @author user-u1
 *
 */
public class PageRankSort2 {
	
	//第一次转换成ID PR,IDLIST格式
	public static class PageRankMapper extends Mapper<Object, Text, MyDoubleWritable, Text>{
			
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String id = itr.nextToken();							//ID
				Text nid = new Text(id);
				double pr = Double.parseDouble(itr.nextToken());			//PR
				DoubleWritable prd = new DoubleWritable(pr);
				MyDoubleWritable d = new MyDoubleWritable(prd, nid);		//自定义类型(pr,id) 先根据pr排序，然后根据id排序
				context.write(d, nid); 	//原来的PR值
			}
			
		}
	}
	
	public static class PageRankReducer extends Reducer<MyDoubleWritable, Text, Text, Text>{
		@Override
		protected void reduce(MyDoubleWritable key, Iterable<Text> values, Reducer<MyDoubleWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value : values){
				context.write(key.getId(), new Text(""+key.getValue().get()));
			}
		}
	}
		
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = PageRankJob2.config();
        
        String input = path.get("Pr");
        String output = path.get("sort");

        Job job = new Job(conf);
        job.setJarByClass(PageRankSort2.class);

        
        job.setMapOutputKeyClass(MyDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
        

    }

}
