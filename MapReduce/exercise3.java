import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.Writable;

public class exercise3 extends Configured implements Tool {

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
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
  	
		public void map(LongWritable key, Text row_of_data, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        
			// Convert the row of data into a string
			String line = row_of_data.toString(); // Convert row_of_data to string
			// Split row of data into words so we can check if the second/third one is a valid year
			String[] words = line.split(",");

			String song_title = words[0];
			String artist_name = words[2];
			String duration = words[3];
			String year = words[165];

			if (isProperYear(year)){ // Check if valid year value

				int year_int = Integer.parseInt(year);

				if ( year_int <= 2010 && year_int >= 2000 ){

					String my_key = String.join("," , song_title, artist_name);
					Text my_text_key = new Text(my_key);
					Text my_text_value = new Text(duration);

					output.collect(my_text_key, my_text_value);
				}
			}		
		}
	}
// <------ MAP CLASS END ------>

// <------ JOB CONFIGURATION ------>
	   

	   public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise3.class);
		conf.setJobName("exercise3");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);	
	    conf.setNumReduceTasks(0);
	   
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
			
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
	    }
	

	    public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new exercise3(), args);
			System.exit(res);
	    }
	}
