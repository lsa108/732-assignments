//to calculate the average score 

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static LongPairWritable output = new LongPairWritable();
		private Text outputKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
        
            JSONObject record = new JSONObject(value.toString());
            long commentCount = 1;
            long score = (Integer) record.get("score");
            output.set(commentCount, score);
            outputKey.set((String) record.get("subreddit"));
            context.write(outputKey, output);

		}
	}

	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable sumResult = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sumComments = 0;
            long sumScore = 0;
			for (LongPairWritable val : values) {
				sumComments += val.get_0();
                sumScore += val.get_1();
			}
			sumResult.set(sumComments, sumScore);
			context.write(key, sumResult);
		}
	}

    public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable averageResult = new DoubleWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			long sumComments = 0;
			long sumScore = 0;
			double averageScore = 0;
            for (LongPairWritable value : values) {
            	sumComments += value.get_0();
            	sumScore += value.get_1();
            }
            averageScore = 1.0 * sumScore/sumComments;
            averageResult.set(averageScore);
            context.write(key, averageResult);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);

        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}