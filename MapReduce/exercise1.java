// TO DO
// - If I don't need them (which I think is the correct way to go) then I should correct my code below to make sure the value passed from Map() is "<#Occurences, #Volumes>", which will be parsed and added to Numerator & denominator in the reduce
// 		- Under this scenario I will need to change the OutputCollector for Map() and the Iterator for Reduce()


import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
// import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { // Assuming key is Text and the Value is an Integer

	// <--- FUNCTION TO CHECK IF VALID YEAR ---->
	// 				<--- START ---->
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
	// 				<--- END ---->
					
	public void map(LongWritable key, Text row_of_data, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
								
		// Convert the row of data into a string
		String line = row_of_data.toString(); // Convert row_of_data to string
		// Split row of data into words so we can check if the second/third one is a valid year
		String[] words = line.split("\\s+");

		// Find the length of the list of parts
		int wordCount = words.length;
// <------DEBUG CODE START------>
	 //    int volumeAsInt = Integer.parseInt(words[3]);

	 //    IntWritable volume = new IntWritable();
	 //    volume.set(volumeAsInt); // set Value

	 //    Text outputKey = new Text(words[1]); // set Key

		// output.collect(outputKey,volume);	
// <------DEBUG CODE END------>	
			// If the count is 4, then we are in the 1_gram scenario
			if (wordCount == 4) {
		//		Check if the second word can be converted to an year
				if(isProperYear(words[1])){
					// Get the number of volumes
					IntWritable volume = new IntWritable(Integer.parseInt(words[3]));
					// Check if 'nu' is in the string
					if(words[0].toLowerCase().contains("nu")){
						String newKey = String.join(",",words[1],"nu");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[1],nu"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'chi' is in the string
					if(words[0].toLowerCase().contains("chi")){
						String newKey = String.join(",",words[1],"chi");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[1],chi"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'haw' is in the string
					if(words[0].toLowerCase().contains("haw")){
						String newKey = String.join(",",words[1],"haw");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[1],haw"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}

				}
			}
		// 	If the count is 5, then we are in the 2_gram scenario
			if (wordCount == 5) {
		//		Check if the third word can be converted to an year
				if(isProperYear(words[2])){
					// Get the number of volumes
					IntWritable volume = new IntWritable(Integer.parseInt(words[4]));
					// Check if 'nu' is in the first string
					if(words[0].toLowerCase().contains("nu")){
						String newKey = String.join(",",words[2],"nu");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],nu"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'chi' is in the first string
					if(words[0].toLowerCase().contains("chi")){
						String newKey = String.join(",",words[2],"chi");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],chi"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'haw' is in the first string
					if(words[0].toLowerCase().contains("haw")){
						String newKey = String.join(",",words[2],"haw");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],haw"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}

					// Check if 'nu' is in the second string
					if(words[1].toLowerCase().contains("nu")){
						String newKey = String.join(",",words[2],"nu");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],nu"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'chi' is in the second string
					if(words[1].toLowerCase().contains("chi")){
						String newKey = String.join(",",words[2],"chi");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],chi"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}
					// Check if 'haw' is in the second string
					if(words[1].toLowerCase().contains("haw")){
						String newKey = String.join(",",words[2],"haw");
						Text yearSyllableKey = new Text(newKey); // Define key as "words[2],haw"
						output.collect(yearSyllableKey, volume); // Add the yearSyllableKey:one pair to the list of key:value pairs i.e. add year-syllable key with value of 1
					}

				}
			}

	    }

	}


    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {
    					// Key;  list of values for a specific key (after the shuffle step) more specifically an iterator
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    // Initialize numerator & denominator
	    float numerator = 0;
	    float denominator = 0;

	    while (values.hasNext()){
	    	numerator += values.next().get();
	    	denominator += 1;
	    }
	    // Calculating average value
	    FloatWritable outputValue = new FloatWritable();
		outputValue.set(numerator/denominator);

// <------DEBUG CODE START------>
	 //    int sum = 0;
	 //    while (values.hasNext()){
	 //    	sum += values.next().get();
	 //    }

		// FloatWritable outputValue = new FloatWritable();
		// outputValue.set( (float) sum);
// <------DEBUG CODE END------>		
	    output.collect(key, outputValue);
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise1.class);
	conf.setJobName("exercise1");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(IntWritable.class);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	conf.setMapperClass(Map.class);
	// conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise1(), args);
	System.exit(res);
    }
}