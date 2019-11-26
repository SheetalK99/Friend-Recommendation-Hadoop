import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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
public class Top10Friends {

	public static class CustomComparator extends WritableComparator {

		// Constructor.

		protected CustomComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable k1 = (IntWritable) w1;
			IntWritable k2 = (IntWritable) w2;

			return -1 * k1.compareTo(k2);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text key_pair = new Text();
		private Text friends = new Text();

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

			String temp[] = value.toString().split("\t");
			if (temp.length == 2) {
				String u1 = temp[0];
				friends.set(temp[1].trim());
				// parse through friend list
				for (String u2 : temp[1].split(",")) {

					// create keys (k1,k2) where k1<k2
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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			HashMap<String, Integer> tracker = new HashMap<>();
			StringBuilder commonFriendsStr = new StringBuilder("");

			int count = 0;
			for (Text friends : values) {
				List<String> temp = Arrays.asList(friends.toString().split(","));
				for (String friend : temp) {
					// count mutual friends and create list
					if (tracker.containsKey(friend)) {
						count++;
						commonFriendsStr.append(friend + ',');

					} else
						tracker.put(friend, 1);

				}
			}
			if (commonFriendsStr.lastIndexOf(",") > -1) {
				commonFriendsStr.deleteCharAt(commonFriendsStr.lastIndexOf(","));
			}
			if (count > 0) {
				result.set(count + "\t" + commonFriendsStr);
				output.write(key, result); // create a pair <keyword, number of occurences>

			}
		}
	}

	// chain job

	// Mapper2 gets input from output of 1st reduce

	public static class Mapper2 extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {

			String temp[] = values.toString().split("\t");

			int newVal = Integer.parseInt(temp[0]);
			count.set(newVal);
			String friendVal = key.toString() + "\t" + temp[1];

			// key as count of mutual friends, same count will go to same reducer
			context.write(count, new Text(friendVal));
		}
	}

	public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> {

		int count = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				String temp[] = val.toString().split("\t");

				String no_friends = key.toString();
				String friend_list = temp[1];
				String pair = temp[0];

				if (count == 10) { // since descending comparator is used top 10 will arrive first
					break;
				}

				else {
					count++;

					context.write(new Text(pair), new Text(no_friends + "\t" + friend_list));

				}

			}

		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		@SuppressWarnings("deprecation")

		// setup 1st job

		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Incorrect parameters");
			System.exit(2);
		}

		// create a job with name "Mutual Friends"
		Job job1 = new Job(conf1, "Top10Friends P1");

		job1.setJarByClass(Top10Friends.class);
		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		// set output key type
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		// Wait till job completion
		if (job1.waitForCompletion(true)) {
			// SETUP 2nd job
			Configuration conf2 = new Configuration();

			Job job2 = new Job(conf2, "Top10MutualFriends");
			job2.setJarByClass(Top10Friends.class);
			job2.setJobName("Top 10 Mutual Friends P2");

			job2.setJarByClass(Top10Friends.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class); // for desceding order

			job2.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}