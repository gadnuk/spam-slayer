import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

	public class PrioriReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
	{
		double spam = 0.0;
		double ham = 0.0;
		double total = 0.0;
		double wordSpam[] = new double[20000];
		double wordHam[] = new double[20000];
		double wordTotal[] = new double[20000];
		MultipleOutputs<Text,DoubleWritable> mos;
		
		public void setup(Context context) {
			mos = new MultipleOutputs<Text,DoubleWritable>(context);
		}
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
			
			double sum = 0.0;

			if (key.toString().equals("spamEmails") || key.toString().equals("hamEmails") || key.toString().equals("totalEmails")
					|| key.toString().equals("spamTokens") || key.toString().equals("hamTokens") || key.toString().equals("totalTokens")) {
				for (DoubleWritable val : values) 
				{
					sum += Double.parseDouble(val.toString());
				}
				
				mos.write("STATS", key, new DoubleWritable(sum));
			}	
			else {
				double sumSq = 0.0;
				int count = 0;
				for (DoubleWritable val : values) 
				{
					sum += Double.parseDouble(val.toString());
					sumSq += Double.parseDouble(val.toString()) * Double.parseDouble(val.toString());
					count++;
				}
				
				double mean = sum/count;
				String arr[] = key.toString().split("_");

				if (key.toString().startsWith("spam_")) {
					wordSpam[Integer.parseInt(arr[1])] = sum;
				}
				if (key.toString().startsWith("ham_")) {
					wordHam[Integer.parseInt(arr[1])] = sum;
				}
				if (key.toString().startsWith("total_")) {
					wordTotal[Integer.parseInt(arr[1])] = sum;
				}
				mos.write("MEAN", new Text(arr[1]), new DoubleWritable(mean));
				mos.write("STDDEV", new Text(arr[1]), new DoubleWritable(Math.sqrt(Math.abs(sumSq - mean * sum) / count)));
			}
			
			if (key.toString().equals("spamEmails")) {
				spam = sum;
			}
			
			if (key.toString().equals("hamEmails")) {
				ham = sum;
			}
			
			if (key.toString().equals("totalEmails")) {
				total = sum;
			}
        }
		
	    
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	double pspam = spam / total;
	    	double pham = ham/total;
	    	double ss = 0.0;
	    	double hs = 0.0;
	    	double ts = 0.0;
	    	
	    	for (int i = 0; i < wordTotal.length; i++) {
	    		ss += wordSpam[i];
	    		hs += wordHam[i];
	    		ts += wordTotal[i];
	    		mos.write("PROBSPAM", new Text(Integer.toString(i)), new DoubleWritable(Math.log(wordSpam[i]/wordTotal[i])));
	    		mos.write("PROBHAM", new Text(Integer.toString(i)), new DoubleWritable(Math.log(wordHam[i]/wordTotal[i])));
	    	}
	    	
	    	mos.write("STATS", new Text("P(spam)"), new DoubleWritable(Math.log(pspam)));
	    	mos.write("STATS", new Text("P(ham)"), new DoubleWritable(Math.log(pham)));
	    	
	    	mos.write("STATS", new Text("Computed Spam Tokens: "), new DoubleWritable(ss));
	    	mos.write("STATS", new Text("Computed Ham Tokens: "), new DoubleWritable(hs));
	    	mos.write("STATS", new Text("Computed Total Tokens: "), new DoubleWritable(ts));
			try {
				mos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}