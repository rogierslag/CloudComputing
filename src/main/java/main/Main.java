package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import scheduler.Scheduler;
import workload.WorkLoadGenerator;
import amazon.Credentials;

@Slf4j
public class Main {

	@Getter
	private Properties properties;
	@Getter
	private Credentials credentials;

	public Main() {
		// Read in the properties.
		properties = readProperties();
		// Create the credential file.
		credentials = new Credentials(properties);
	}

	public void start() {
		// Create the Scheduler.
		new Scheduler(credentials, properties);
		// Create the workload generator.
		WorkLoadGenerator wlg = new WorkLoadGenerator(credentials, properties);
		new Thread(wlg).start();
	}

	/**
	 * Reads the properties file and load them into a Properties object
	 * 
	 * @return the created Properties object
	 */
	private static Properties readProperties() {
		try {
			FileInputStream propertiesFile = new FileInputStream("scheduler.properties");
			Properties props = new Properties();
			props.load(propertiesFile);
			log.info("Working with the following properties: {}", props);
			return props;
		} catch (FileNotFoundException e) {
			// A problem was found, so returning to sane defaults happens in the
			// app
			log.error("Triggered an FileNotFound error when trying to read the properties file.", e);
			return new Properties();
		} catch (IOException e) {
			log.error("Triggered an IO error when trying to read the properties file.", e);
			return new Properties();
		}
	}

	/**
	 * Starts the scheduler and workload generator.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Main main = new Main();
		main.start();
	}
}
