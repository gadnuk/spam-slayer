import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SplitReducer extends Reducer<Text,Text,Text,Text> {
//private IntWritable result = new IntWritable();
	MultipleOutputs<Text,Text> mos;
	
	public void setup(Context context) {
		mos = new MultipleOutputs<Text,Text>(context);
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(key.toString().equals("train")) {
			for (Text t : values) {
				mos.write("TRAIN", null, t);
			}
		}
		else {
			for (Text t : values) {
				mos.write("TEST", null, t);
			}			
		}
	}
	
	public void cleanup(Context context) {
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