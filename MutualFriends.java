import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Author: Sheetal Kadam(sak170006)
//Date: 09/29/2018
public class MutualFriends {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text key_pair = new Text();
		private Text friends = new Text();

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

			String temp[] = value.toString().split("\t");
			if (temp.length == 2) {
				String u1 = temp[0];
				friends.set(temp[1].trim());
				
				//Parse through friend list
				for (String u2 : temp[1].split(",")) {

					int fr1 = Integer.parseInt(u1);
					int fr2 = Integer.parseInt(u2);

					//check if userid matches with the question
					if ((fr1 == 0 && fr2 == 1) || (fr1 == 1 && fr2 == 0) || (fr1 == 20 && fr2 == 28193)
							|| (fr1 == 28193 && fr2 == 20) || (fr1 == 1 && fr2 == 29826) || (fr1 == 29826 && fr2 == 1)
							|| (fr1 == 6222 && fr2 == 19272) || (fr1 == 19272 && fr2 == 6222)
							|| (fr1 == 28041 && fr2 == 28056) || (fr1 == 28056 && fr2 == 28041)) {

						if (Integer.parseInt(u1) < Integer.parseInt(u2)) {
							key_pair.set(u1 + "," + u2);
						} else {
							key_pair.set(u2 + "," + u1);
						}

						output.write(key_pair, friends);

					}
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Text commonFriendsOut = new Text();

		public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			HashMap<String, Integer> tracker = new HashMap<>();
			StringBuilder commonFriendsStr = new StringBuilder("");

			//Itereate through friend list
			for (Text friends : values) {
				List<String> temp = Arrays.asList(friends.toString().split(","));
				for (String friend : temp) {
					//found in second key list so add to mutual friends
					if (tracker.containsKey(friend))
						commonFriendsStr.append(friend + ',');
					else
						tracker.put(friend, 1);

				}
			}

			//cleaning
			if (commonFriendsStr.lastIndexOf(",") > -1) {
				commonFriendsStr.deleteCharAt(commonFriendsStr.lastIndexOf(","));
			}
			if (commonFriendsStr.length() > 0) {
				commonFriendsOut.set(commonFriendsStr.toString());
				output.write(key, commonFriendsOut); // create a pair <keyword, common friends>

			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.out.println(otherArgs[0]);
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}