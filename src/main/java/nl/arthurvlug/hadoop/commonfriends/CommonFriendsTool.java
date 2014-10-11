package nl.arthurvlug.hadoop.commonfriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommonFriendsTool extends Configured implements Tool {
	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        
        Job job = new Job(conf);
        job.setJarByClass(CommonFriendsTool.class);

        job.setMapperClass(CommonFriendsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setCombinerClass(CommonFriendsReducer.class);
        job.setReducerClass(CommonFriendsReducer.class);
        job.setOutputKeyClass(FriendsPair.class);
        job.setOutputValueClass(ArrayWritable.class);

        job.setCombinerClass(CommonFriendsAggregationReducer.class);
        job.setReducerClass(CommonFriendsAggregationReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
	}

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CommonFriendsTool(), args);
        System.exit(res);
    }
}
