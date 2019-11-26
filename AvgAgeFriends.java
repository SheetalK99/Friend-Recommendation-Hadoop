import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AvgAgeFriends {

	public static class CustomComparator extends WritableComparator {

		// Constructor.

		protected CustomComparator() {
			super(LongWritable.class, true);
		}

		@SuppressWarnings("rawtypes")

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			LongWritable k1 = (LongWritable) w1;
			LongWritable k2 = (LongWritable) w2;

			return -1 * k1.compareTo(k2);
		}
	}

	public static class MapAge extends Mapper<Object, Text, Text, Text> {

		private Text user = new Text();
		private Text avg_age = new Text();

		static HashMap<String, Integer> dataDict = new HashMap<>();

		public int getAge(String dobStr) throws ParseException {

			Date now = new Date();

			Date dob = new SimpleDateFormat("mm/dd/yyyy").parse(dobStr);
			long timeBetween = now.getTime() - dob.getTime();
			double yearsBetween = timeBetween / 3.15576e+10;
			return (int) Math.floor(yearsBetween);

		}
		protected void setup(Context context) throws IOException, InterruptedException {
			
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path data_path = new Path(context.getConfiguration().get("Userdata"));// Location of file in HDFS
		FileSystem f = FileSystem.get(conf);
		FileStatus[] fstatus = f.listStatus(data_path);

		for(
		FileStatus status:fstatus)
		{
			Path pt = status.getPath();

			BufferedReader br = new BufferedReader(new InputStreamReader(f.open(pt)));
			String line;
			line = br.readLine();
			// read file
			while (line != null) {
				String[] data = line.split(",");
				if (data.length == 10) {

					try {
						dataDict.put(data[0], getAge(data[9]));
					} catch (ParseException e) {

						e.printStackTrace();
					}
				}
				line = br.readLine();
			}
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String temp[] = value.toString().split("\t");
		if (temp.length == 2) {
			String u1 = temp[0];
			int sum = 0;
			int count = 0;

			for (String f : temp[1].split(",")) {
				sum = sum + dataDict.get(f);
				count++;
			}

			user.set(u1);
			avg_age.set("Age:" + "\t" + (sum / count));
			context.write(user, avg_age);//output user and average age of friends

		}

	}
}

public static class MapAddress extends Mapper<Object, Text, Text, Text> {

	private Text user = new Text();
	private Text user_addr = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String temp[] = value.toString().split(",");
		if (temp.length == 10) {
			String u1 = temp[0];
			String address = temp[3] + "," + temp[4] + "," + temp[5];
			user_addr.set("Addr:" + "\t" + address);
			user.set(u1);
			context.write(user, user_addr);//output user and address

		}

	}
}

public static class Reduce extends Reducer<Text, Text, Text, Text> {

	
	//Reduce side join
	
	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
		String address = "";
		String age = "";
		for (Text t : values) {
			//create address,age value
			String[] val = t.toString().split("\t");
			
			if (val[0].equals("Addr:")) {

				address = val[1];

			} 
			
			else if (val[0].equals("Age:")) {
				age = val[1];
			}
		}
		result.set(address + "\t" + age);
		output.write(key, result);
	}
}
// chain job

// Mapper2

public static class Mapper2 extends Mapper<Text, Text, LongWritable, Text> {

	//sort by descending order, change key to average age
	private LongWritable avg_age = new LongWritable();
	private Text new_value = new Text();

	public void map(Text key, Text values, Context context) throws IOException, InterruptedException {

		// System.out.println(values.toString());

		String newVal[] = values.toString().split("\t");
		if (newVal.length == 2) {
			int new_key = Integer.parseInt(newVal[1]);
			String new_val = key.toString() + "," + newVal[0];
			new_value.set(new_val);

			avg_age.set(new_key);
			context.write(avg_age, new_value);
		}
	}
}

public static class Reducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {

	int count = 0;
	//keep top 15 pairs of users by average age

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {

			if (count == 15) {
				break;
			}

			else {
				count++;
				context.write(val, key);
			}

		}

	}

	}

	// Driver program
	public static void main(String[] args) throws Exception {

		@SuppressWarnings("deprecation")

		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Incorrect parameters");
			System.exit(2);
		}
		conf1.set("Userdata", otherArgs[1]);

		// create a job with name "Mutual Friends"
		Job job1 = new Job(conf1, "Average Age");

		job1.setJarByClass(AvgAgeFriends.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(Text.class);

		// Set the two mappers
		
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, MapAddress.class);

		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, MapAge.class);

		Path outputPath = new Path(args[2]);

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, outputPath);

		// Wait till job completion

		if (job1.waitForCompletion(true)) {

			Configuration conf2 = new Configuration();

			Job job2 = new Job(conf2, "Average age of friends");
			job2.setJarByClass(AvgAgeFriends.class);
			job2.setJobName("Average age of friends");

			job2.setJarByClass(AvgAgeFriends.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);

			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			//descending sort
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job2.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}
}