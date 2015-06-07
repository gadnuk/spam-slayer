import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ConverterUtils.DataSource;



public class Training {

	public static void main(String[] args) throws Exception {
	
		Instances train = DataSource.read("/Users/eric/Desktop/train.arff");
		train.setClassIndex(21);
		Instances test = DataSource.read("/Users/eric/Desktop/test.arff");
		test.setClassIndex(21);
		// train classifier
		J48 cls = new J48();
		cls.buildClassifier(train);
		// output predictions
		System.out.println("# - actual - predicted - distribution");
		for (int i = 0; i < test.numInstances(); i++) {
			double pred = cls.classifyInstance(test.instance(i));
			double[] dist = cls.distributionForInstance(test.instance(i));
			System.out.print((i+1) + " - ");
			System.out.print(test.instance(i).toString(test.classIndex()) + " - ");
			System.out.print(test.classAttribute().value((int) pred) + " - ");
			System.out.println(Utils.arrayToString(dist));
		}
	}
}