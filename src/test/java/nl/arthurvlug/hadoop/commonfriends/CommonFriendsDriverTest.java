package nl.arthurvlug.hadoop.commonfriends;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class CommonFriendsDriverTest {
	private Configuration conf;
	private Path input;
	private Path output;
	private FileSystem fs;

	@Before
	public void setup() throws IOException {
		conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");

		input = new Path("src/test/resources/input");
		output = new Path("target/output");

		fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
	}

	@Test
	public void test() throws Exception {
		CommonFriendsTool numberSort = new CommonFriendsTool();
		numberSort.setConf(conf);

		int exitCode = numberSort.run(new String[] { input.toString(), output.toString() });
		assertEquals(0, exitCode);

		validateOuput();
	}

	private void validateOuput() throws IOException {
		InputStream in = null;
		try {
			in = fs.open(new Path("target/output/part-r-00000"));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			assertEquals("(A B) -> (C D)", br.readLine());
			assertEquals("(A C) -> (B D)", br.readLine());
			assertEquals("(A D) -> (B C)", br.readLine());
			assertEquals("(B C) -> (A D E)", br.readLine());
			assertEquals("(B D) -> (A C E)", br.readLine());
			assertEquals("(B E) -> (C D)", br.readLine());
			assertEquals("(C D) -> (A B E)", br.readLine());
			assertEquals("(C E) -> (B D)", br.readLine());
			assertEquals("(D E) -> (B C)", br.readLine());

		} finally {
			IOUtils.closeStream(in);
		}
	}
}
