package nl.arthurvlug.hadoop.commonfriends;

import org.apache.hadoop.io.Text;

public class UserComparator {
	public static Text smallest(Text user1, Text user2) {
		if(user1.compareTo(user2) > 0) {
			return user2;
		} else {
			return user1;
		}
	}

	public static Text biggest(Text user1, Text user2) {
		if(smallest(user1, user2) == user1) {
			return user2;
		} else {
			return user1;
		}
	}
}
