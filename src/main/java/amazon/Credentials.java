package amazon;

import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;

public class Credentials implements AWSCredentials {

	private final String awsAccessKey;
	private final String awsSecretKey;

	public Credentials(Properties properties) {
		awsAccessKey = properties.getProperty("aws.s3.access_key", null);
		awsSecretKey = properties.getProperty("aws.s3.secret_key", null);
	}

	public String getAWSAccessKeyId() {
		return awsAccessKey;
	}

	public String getAWSSecretKey() {
		return awsSecretKey;
	}

}
