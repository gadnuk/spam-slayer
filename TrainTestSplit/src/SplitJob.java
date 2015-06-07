


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SplitJob extends Configured implements Tool {

	//public static HashMap<Integer, String> hm = new HashMap<Integer, String>();
	public static int lineCounter = 0;
	public static final int testCount = 1000;
	public static double testRatio = 0;
	
	/**
	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SplitJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapred.map.child.java.opts", "-Xmx3000m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx3000m");
        Job job3 = new Job(conf, "Split input data into Train and Test");
        job3.setJarByClass(SplitJob.class);
        job3.setMapperClass(SplitMapper.class);
        job3.setReducerClass(SplitReducer.class);
		//job3.setPartitionerClass(SplitKeyPartitioner.class);
		//job3.setGroupingComparatorClass(SplitGroupingComparator.class);
        
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(job3, "TRAIN", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job3, "TEST", TextOutputFormat.class, Text.class, Text.class);
        FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));		
		job3.waitForCompletion(true);
		
		return 0;
	}

}
