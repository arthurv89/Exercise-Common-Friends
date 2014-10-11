package nl.arthurvlug.hadoop.commonfriends;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable inputkey, Text inputValue, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());
		Text user = new Text(tokenizer.nextToken());
		String friend;
		while(tokenizer.hasMoreTokens()) {
			friend = tokenizer.nextToken();
			Text friendText = new Text(friend);
			
			Text smallest = UserComparator.smallest(user, friendText);
			Text biggest = UserComparator.biggest(user, friendText);
			
			context.write(smallest, biggest);
		}
	}

}
