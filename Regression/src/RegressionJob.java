


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RegressionJob extends Configured implements Tool {
	
	public static class LinearRegressionMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable>
	{


		protected void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
	
			/**
			*
			* Linear-Regression costs function
			*
			* This will simply sum over the subset and calculate the predicted value
			* y_predict(x) for the given features values and the current theta values
			* Then it will subtract the true y values from the y_predict(x) value for
			* every input record in the subset
			*
			* J(theta) = sum((y_predict(x)-y)^2)
			* y_predict(x) = theta(0)*x(0) + .... + theta(i)*x(i)
			*
			*/
		
			String line = value.toString();
			String[] features = line.split(",");
		
			ArrayList<Float> values = new ArrayList<Float>();
		
			/**
			* read the values and convert them to floats
			*/
			for(int i = 0; i<features.length; i++)
			{
				values.add(new Float(features[i]));
			}
		
			/**
			* calculate the costs
			*
			*/
		
			context.write(new LongWritable(1), new FloatWritable(costs(values)));
	
		}
	
		private final float costs(ArrayList<Float> values)
		{
			/**
			* Load the cache files
			*/
	
		
			float costs = 0;
	
				
				String t = new String("0.01,0.03,4,2,0.9,2,0.8,0.9,2,3,0.1");
				//all right we have all the theta values, lets convert them to floats
				String[] theta = t.split(",");
			
				//first value is the y value
				float y = (Float) values.get(0);
			
				/**
				* Calculate the costs for each record in values
				*/
				for(int j = 0; j < values.size(); j++)
				{
			
				//bias calculation
				if(j == 0)
					costs += (new Float(theta[j])) * 1;
					else
					costs += Float.parseFloat(theta[j]) * (Float) values.get(j);
			
				}
			
				// Subtract y and square the costs
				//costs = (costs -y)*(costs - y);
		
			return costs;
	
		}

	}
	
	
	public static class LinearRegressionReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable>
	{

		public void reduce(LongWritable key, Iterator<FloatWritable> value, Context context) throws IOException, InterruptedException {
	
			/**
			* The reducer just has to sum all the values for a given key
			*
			*/
		
			float sum = 0;
		
			while(value.hasNext())
			{
				sum += value.next().get();
			}
		
			context.write(key, new FloatWritable(sum));
		
		}

	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RegressionJob(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		conf.set("mapred.map.child.java.opts", "-Xmx3000m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx3000m");
		Job job = new Job(conf, "Test the data");
		job.setJarByClass(RegressionJob.class);
		job.setMapperClass(LinearRegressionMapper.class);
		job.setReducerClass(LinearRegressionReducer.class);

		job.setNumReduceTasks(1);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		// job.setNumReduceTasks(1);
		return 0;
	}


}
