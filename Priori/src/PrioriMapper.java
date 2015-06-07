import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrioriMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		
        	String [] lineArr = value.toString().split(" ");
        	int spamTokens = 0;
        	int hamTokens = 0;
        	int totalTokens = 0;
        	
        	if (value.toString().equals("") || value.toString().equals(null)) {
        		return;
        	}
        	
	        if (Integer.parseInt(lineArr[lineArr.length-1]) == 1) {
	        	for (int i = 0; i < lineArr.length-1; i++) {
	        		spamTokens += Integer.parseInt(lineArr[i]);
	        		context.write(new Text(new String("spam_"+i)), new DoubleWritable(Double.parseDouble(lineArr[i])));
	        	}
	        	context.write(new Text("spamTokens"), new DoubleWritable(spamTokens));
	        	context.write(new Text("spamEmails"), new DoubleWritable(1.0));
	        }
	        else {
	        	for (int i = 0; i < lineArr.length-1; i++) {
	        		hamTokens += Integer.parseInt(lineArr[i]);
	        		context.write(new Text(new String("ham_"+i)), new DoubleWritable(Double.parseDouble(lineArr[i])));

	        	}
	        	context.write(new Text("hamTokens"), new DoubleWritable(hamTokens));
	        	context.write(new Text("hamEmails"), new DoubleWritable(1.0));
	        }
	        
	    	for (int i = 0; i < lineArr.length-1; i++) {
	    		totalTokens += Integer.parseInt(lineArr[i]);
        		context.write(new Text(new String("total_"+i)), new DoubleWritable(Double.parseDouble(lineArr[i])));
	    	}
	    	context.write(new Text("totalTokens"), new DoubleWritable(totalTokens));
	    	context.write(new Text("totalEmails"), new DoubleWritable(1.0));
    }


}