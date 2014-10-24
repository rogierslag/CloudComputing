package workload;

import java.io.IOException;

import org.junit.Test;

public class TestVideoTest {

	@Test(expected = IOException.class)
	public void TestVideo_IOException() throws IOException {
		// Act
		new TestTask("NONEXISTINGFILE", "NaN");
	}

}
