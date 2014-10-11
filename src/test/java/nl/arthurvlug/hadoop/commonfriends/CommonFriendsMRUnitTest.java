package nl.arthurvlug.hadoop.commonfriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CommonFriendsMRUnitTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, FriendsPair, Text> mapReduceDriver;

	@Before
	public void setup() {
		Mapper<LongWritable, Text, Text, Text> mapper = new CommonFriendsMapper();
		Reducer<Text, Text, FriendsPair, Text> reducer = new CommonFriendsReducer();
		Configuration conf = new Configuration();
		
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		mapDriver.setConfiguration(conf);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, FriendsPair, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		mapReduceDriver.setConfiguration(conf);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(0), new Text("A B C D"));

		mapDriver.withOutput(new Text("A"), new Text("B"));
		mapDriver.withOutput(new Text("A"), new Text("C"));
		mapDriver.withOutput(new Text("A"), new Text("D"));
		mapDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		ArrayWritable commonFriends = new ArrayWritable(Text.class);
		commonFriends.set(new Text[] {new Text("A")});
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("A B"));
		mapReduceDriver.withInput(new LongWritable(2), new Text("A C"));
		mapReduceDriver.withInput(new LongWritable(3), new Text("B A"));
		mapReduceDriver.withInput(new LongWritable(4), new Text("C A"));
		mapReduceDriver.addOutput(new FriendsPair(new Text("B"), new Text("C")), commonFriends);
		mapReduceDriver.runTest();
	}
}
