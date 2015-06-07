import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitMapper extends Mapper<Object, Text, Text, Text>{
	private String TRAININGLINES = "";
	private String TESTINGLINES = "";
	private int count = 0;
	private double coinflip = 0.0;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		coinflip = Math.random() * 100;
		if (coinflip < 10) {
			TESTINGLINES = TESTINGLINES.concat(value.toString().concat("\n"));
		}
		else {
			TRAININGLINES = TRAININGLINES.concat(value.toString().concat("\n"));
		}
		count++;
		if(count >=3000)
		{
			context.write(new Text("train"),new Text(TRAININGLINES));
			context.write(new Text("test"),new Text(TESTINGLINES));
			TRAININGLINES = null;
			TRAININGLINES = "";
			TESTINGLINES = null;
			TESTINGLINES = "";
			count = 0;
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (TRAININGLINES != null) {
			context.write(new Text("train"),new Text(TRAININGLINES));
		}
		if (TESTINGLINES != null) {
			context.write(new Text("test"),new Text(TESTINGLINES));
		}
		TRAININGLINES = null;
		TESTINGLINES = null;
		TRAININGLINES = "";
		TESTINGLINES = "";
		count = 0;
	}
}