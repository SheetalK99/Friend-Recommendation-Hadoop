
//Author: Sheetal Kadam (sak170006)
//Date: 09/29/2019
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendStates {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text key_pair = new Text();
		private Text friends = new Text();

		static HashMap<Integer, String> dataDict = new HashMap<Integer, String>();

		// load user data for in memory join
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path data_path = new Path(context.getConfiguration().get("Data"));// Location of file in HDFS
			FileSystem f = FileSystem.get(conf);
			FileStatus[] fstatus = f.listStatus(data_path);

			for (FileStatus status : fstatus) {
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(f.open(pt)));
				String line;
				line = br.readLine();
				// read file
				while (line != null) {
					String[] data = line.split(",");
					if (data.length == 10) {
						// get name and address of each user
						dataDict.put(Integer.parseInt(data[0]), data[1] + ":" + data[5]);
					}
					line = br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

			String temp[] = value.toString().split("\t");

			if (temp.length == 2) {

				Configuration conf_1 = output.getConfiguration();

				// command line data input
				String input_friend1 = (conf_1.get("InputFriend1"));
				String input_friend2 = (conf_1.get("InputFriend2"));

				String f1 = temp[0];
				StringBuilder friendsState = new StringBuilder("");

				for (String f : temp[1].split(",")) {
					friendsState.append(dataDict.get(Integer.parseInt(f)) + ","); // create list
				}

				if (friendsState.lastIndexOf(",") > -1) {
					friendsState.deleteCharAt(friendsState.lastIndexOf(","));
				}
				friends.set(friendsState.toString());

				for (String f2 : temp[1].split(",")) {

					// check if friend matches input list
					if ((Integer.parseInt(f2) == Integer.parseInt(input_friend2)
							&& Integer.parseInt(f1) == Integer.parseInt(input_friend1))
							|| (Integer.parseInt(f1) == Integer.parseInt(input_friend2)
									&& Integer.parseInt(f2) == Integer.parseInt(input_friend1))) {

						if (Integer.parseInt(f1) < Integer.parseInt(f2)) {
							key_pair.set(f1 + "," + f2);
						} else {
							key_pair.set(f2 + "," + f1);
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

			for (Text friends : values) {
				List<String> temp = Arrays.asList(friends.toString().split(","));
				for (String friend : temp) {
					if (tracker.containsKey(friend))
						commonFriendsStr.append(friend + ','); // mutual friend list
					else
						tracker.put(friend, 1);

				}
			}

			if (commonFriendsStr.lastIndexOf(",") > -1) {
				commonFriendsStr.deleteCharAt(commonFriendsStr.lastIndexOf(","));
			}
			if (commonFriendsStr.length() > 0) {
				commonFriendsOut.set(commonFriendsStr.toString());
				output.write(key, commonFriendsOut); // create a pair <keyword, state>

			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.out.println(otherArgs[0]);
			System.err.println("Incorrect parameters");
			System.exit(2);
		}

		conf1.set("InputFriend1", otherArgs[3]);
		conf1.set("InputFriend2", otherArgs[4]);

		conf1.set("Data", otherArgs[1]);

		Job job = new Job(conf1, "Friend States");
		job.setJarByClass(FriendStates.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}