import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestBayesJob extends Configured implements Tool {
	public static class TestBayesMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {
		double pspam = 0.0;
		double pham = 0.0;
		int id = 0;

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			double newspam = -0.5574163094999631;
			double newham = -0.8502380439104454;
			Configuration conf = new Configuration();
			conf = context.getConfiguration();
			Path[] pathsOfFiles = DistributedCache.getLocalCacheFiles(conf);
			String pathToProbSpam = pathsOfFiles[0].toString();
			String pathToProbHam = pathsOfFiles[1].toString();
			String pathToStats = pathsOfFiles[2].toString();
			BufferedReader br1 = new BufferedReader(new FileReader(
					pathToProbSpam));
			BufferedReader br2 = new BufferedReader(new FileReader(
					pathToProbHam));
			BufferedReader b32 = new BufferedReader(new FileReader(pathToStats));
			//
			String[] lineArr = value.toString().split(" ");
			if (value.toString().equals("") || value.toString().equals(null)) {
				br1.close();
				br2.close();
				b32.close();
				return;
			}
			String temp = b32.readLine();
			while (temp != null) {
				// System.out.println(temp);
				String arr[] = temp.split("\\s+");
				if (arr[1].equals("P(spam)")) {
					pspam = Double.parseDouble(arr[1]);
				}
				if (arr[1].equals("P(ham)")) {
					pham = Double.parseDouble(arr[1]);
				}
				temp = b32.readLine();
			}
			for (int i = 0; i < lineArr.length; i++) {
				while (br1.readLine() != null) {
					String str = br1.readLine().replaceAll("\\s+", " ");
					String arr[] = str.split(" ");
					if (Integer.parseInt(arr[0]) == i) {
						newspam += Double.parseDouble(arr[1]);
					}
				}
				while (br2.readLine() != null) {
					String str2 = br2.readLine().replaceAll("\\s+", " ");
					String arr[] = str2.split(" ");
					if (Integer.parseInt(arr[0]) == i) {
						newham += Double.parseDouble(arr[1]);
					}
				}
			}
			newham += pham;
			newspam += pspam;
			if (newham > newspam) {
				if (Integer.parseInt(lineArr[lineArr.length - 1]) == 0) {
					context.write(new Text("correct"), new DoubleWritable(1.0));
				} else {
					context.write(new Text("wrong"), new DoubleWritable(1.0));
				}
			} else {
				if (Integer.parseInt(lineArr[lineArr.length - 1]) == 1) {
					context.write(new Text("correct"), new DoubleWritable(1.0));
				} else {
					context.write(new Text("wrong"), new DoubleWritable(1.0));
				}
			}
			context.write(new Text("total"), new DoubleWritable(1.0));
			br1.close();
			br2.close();
			b32.close();
		}
	}

	public static class TestBayesReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		double correct = 0.0;
		double wrong = 0.0;
		double total = 0.0;

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += Double.parseDouble(val.toString());
			}
			context.write(key, new DoubleWritable(sum));
			if (key.toString().equals("correct")) {
				correct = sum;
			}
			if (key.toString().equals("wrong")) {
				wrong = sum;
			}
			if (key.toString().equals("total")) {
				total = sum;
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			double accuracy = 0.0;
			double errorRate = 0.0;
			accuracy = (correct / total) * 100.0;
			errorRate = (wrong / total) * 100.0;
			context.write(new Text("Accuracy: "), new DoubleWritable(accuracy));
			context.write(new Text("Error Rate: "), new DoubleWritable(
					errorRate));
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TestBayesJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		conf.set("mapred.map.child.java.opts", "-Xmx3000m");
		conf.set("mapred.reduce.child.java.opts", "-Xmx3000m");
		Job job = new Job(conf, "Test the data");
		job.setJarByClass(TestBayesJob.class);
		job.setMapperClass(TestBayesMapper.class);
		job.setReducerClass(TestBayesReducer.class);
		DistributedCache.addCacheFile(new Path(args[2]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[3]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[4]).toUri(),
				job.getConfiguration());
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		MultipleOutputs.addNamedOutput(job, "OUTPUT", TextOutputFormat.class,
				Text.class, DoubleWritable.class);
		MultipleOutputs.addNamedOutput(job, "ROC", TextOutputFormat.class,
				Text.class, DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		// job.setNumReduceTasks(1);
		return 0;
	}
}