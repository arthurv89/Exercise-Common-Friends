package nl.arthurvlug.hadoop.commonfriends;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFriendsReducer extends Reducer<Text, Text, FriendsPair, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text user1 : values) {
			for(Text user2 : values) {
				if(!user2.equals(user1)) {
					// Determine correct order
					Text smallest = UserComparator.smallest(user1, user2);
					Text biggest = UserComparator.biggest(user1, user2);
					
					// Both users have `key` as common friend.
					context.write(new FriendsPair(smallest, biggest), key);
				}
			}
		}
	}
}
