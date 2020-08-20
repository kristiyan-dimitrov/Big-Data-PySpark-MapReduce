import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Writable;

import java.lang.Math;

public class exercise2 extends Configured implements Tool {
// <--- FUNCTION TO CHECK IF VALID YEAR ---->
	// 		<--- START ---->
	public static boolean isProperYear(String str) {
	    if (str == null) {
	        return false;
	    }
	    int length = str.length();
	    if (length == 0) {
	        return false;
	    }
		if (length >= 5) { // I added this condition to more closely check for years
	        return false;
	    }
	    int i = 0;
	    if (str.charAt(0) == '-') {
	        if (length == 1) {
	            return false;
	        }
	        i = 1;
	    }
	    for (; i < length; i++) {
	        char c = str.charAt(i);
	        if (c < '0' || c > '9') {
	            return false;
	        }
	    }
	    return true;
	}
	// Credit to this SO post: https://stackoverflow.com/questions/237159/whats-the-best-way-to-check-if-a-string-represents-an-integer-in-java?page=1&tab=votes#tab-top
	// Note that overflow shouldn't be a problem here, given the values in the data
	// 		<--- END ---->

// <------ MAP CLASS START ------>
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		Text arbitraryKey = new Text("standardDeviation");

	// This Key is irrelevant, the value is thing we see in the row
	// OutputCollector is a list with key:value pairs where the key is Text and the Value is IntWritable
		public void map(LongWritable key, Text row_of_data, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    
		    // Convert the row of data into a string
			String line = row_of_data.toString(); // Convert row_of_data to string
			// Split row of data into words so we can check if the second/third one is a valid year
			String[] words = line.split("\\s+");
			// Find the length of the list of parts
			int wordCount = words.length;

// 			<------ 1_GRAM FILE START  ------>
			// If the count is 4, then we are in the 1_gram scenario
			if (wordCount == 4) {
				// Check if the second word can be converted to an year
				if(isProperYear(words[1])){
					// Get the number of volumes
					Text volume = new Text(words[3]);
					output.collect(arbitraryKey, volume);
				}
			}
// 			<------ 1_GRAM FILE END  ------>

// 			<------ 2_GRAM FILE START  ------>
			// 	If the count is 5, then we are in the 2_gram scenario
			if (wordCount == 5) {
			//	Check if the third word can be converted to an year
				if(isProperYear(words[2])){
					// Get the number of volumes
					Text volume = new Text(words[4]);
					output.collect(arbitraryKey, volume);
				}
			}
// 			<------ 2_GRAM FILE END  ------>
		}

	}
// <------ MAP CLASS END ------>
// <------ COMBINER CLASS START ------>

    public static class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			double count = 0;
			double sum = 0;
			double sumSq = 0;

		    while (values.hasNext()) {
				count += 1;

				Text volumeText = values.next(); // Get next Text value
				String volumeString = volumeText.toString(); // Convert it to String
				double volume = Double.parseDouble(volumeString); // Convert String to Double
				double volumeSq = volume*volume;
				
				sum += volume;
				sumSq += volumeSq;

		    }
		    
		    String tripletString = String.join(",", Double.toString(count), Double.toString(sum), Double.toString(sumSq)); 
			Text triplet = new Text(tripletString);

		    output.collect(key, triplet);
		}
    }
// <------ COMBINER CLASS END ------>
// <------ REDUCE CLASS START ------>

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

			double countTotal = 0;
			double sumTotal = 0;
			double sumSqTotal = 0;

		  	while (values.hasNext()) {
				
				Text tripletText = values.next(); // Get next Text value
				String tripletString = tripletText.toString(); // Convert it to String
				String[] array = tripletString.split(",");

				double count = Double.parseDouble(array[0]);
				double sum = Double.parseDouble(array[1]);
				double sumSq = Double.parseDouble(array[2]);

				countTotal += count;
				sumTotal += sum;
				sumSqTotal += sumSq;

		    }
		    
			double std = Math.sqrt(sumSqTotal/countTotal - Math.pow(sumTotal/countTotal,2));
			DoubleWritable standardDeviation = new DoubleWritable(std);

		    output.collect(key, standardDeviation);
		}
    }
// <------ REDUCE CLASS END ------>
    public int run(String[] args) throws Exception {
		
		JobConf conf = new JobConf(getConf(), exercise2.class);
		conf.setJobName("wordcount");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise2(), args);
	System.exit(res);
    }
}