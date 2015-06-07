


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PrioriJob extends Configured implements Tool {
	
	/**
	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PrioriJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapred.map.child.java.opts", "-Xmx3000m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx3000m");
        Job job = new Job(conf, "Calculate priories");
        job.setJarByClass(PrioriJob.class);
        job.setMapperClass(PrioriMapper.class);
        job.setReducerClass(PrioriReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        MultipleOutputs.addNamedOutput(job, "STATS", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "STDDEV", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "MEAN", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "PROBSPAM", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "PROBHAM", TextOutputFormat.class, Text.class, DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		job.waitForCompletion(true);
		//job.setNumReduceTasks(1);
		return 0;
	}

}
