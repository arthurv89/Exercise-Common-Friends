package nl.arthurvlug.hadoop.commonfriends;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@AllArgsConstructor
@NoArgsConstructor
public class FriendsPair implements WritableComparable<FriendsPair> {
	@Getter
	private Text user1;
	@Getter
	private Text user2;

	@Override
	public void readFields(DataInput in) throws IOException {
		user1 = new Text(in.readUTF());
		user2 = new Text(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(user1.getBytes());
		out.write(user2.getBytes());
	}

	@Override
	public int compareTo(FriendsPair o) {
		int diff = user1.compareTo(o.user1);
		if(diff != 0) {
			return diff;
		}
		
		return user2.compareTo(o.user2);
	}

}
