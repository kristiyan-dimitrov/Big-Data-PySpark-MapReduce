import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;

public class exercise4 extends Configured implements Tool {

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
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>{

		public void map(LongWritable key, Text row_of_data, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	        
			// Convert the row of data into a string
			String line = row_of_data.toString(); // Convert row_of_data to string
			// Split row of data into words so we can check if the second/third one is a valid year
			String[] words = line.split(",");

			String artist_name = words[2];
			double duration = Double.parseDouble(words[3]);
			String year = words[165];

			if (isProperYear(year)){ // Check if valid year value

				Text my_text_key = new Text(artist_name);
				DoubleWritable duration_writable = new DoubleWritable(duration);

				output.collect(my_text_key, duration_writable);
			
			}		
		}
	}
// <------ MAP CLASS END ------>

// <------ PARTITIONER CLASS START ------>
	public static class Partition implements Partitioner<Text, DoubleWritable> {   

		public void configure(JobConf job) {
    	}

	    @Override
	    public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

	        String artist = key.toString();

	        if(numReduceTasks == 0) {
	            return 0;
	        }

	        if(artist.compareTo(String.valueOf('f')) < 0){ // Reference for comparing Strings
	            return 0; 
	        } else if (artist.compareTo(String.valueOf('k')) < 0){
	            return 1;
	        } else if (artist.compareTo(String.valueOf('p')) < 0){ 
	            return 2;
	        } else if (artist.compareTo(String.valueOf('u')) < 0){ 
	            return 3;
	        } else { 
	        	return 4;
	        }

	    }
	}
// <------ PARTITIONER CLASS END ------>

// <------ REDUCE CLASS START ------>
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

			double max_duration = 0;
			
			while (values.hasNext()) {

        		double duration = values.next().get();

          		if (max_duration < duration) {
            		max_duration = duration;
          		}
        	}
        	
        	output.collect(key, new DoubleWritable(max_duration));
		}
	}
// <------ REDUCE CLASS END ------>

// <------ JOB CONFIGURATION ------>
	   

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise4.class);
		conf.setJobName("exercise4");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);	
		conf.setCombinerClass(Reduce.class);
		conf.setPartitionerClass(Partition.class);
		conf.setReducerClass(Reduce.class);

	    conf.setNumReduceTasks(5);
	   
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
			
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
	}
	

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise4(), args);
		System.exit(res);
    }
}
