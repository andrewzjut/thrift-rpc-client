package com.trcloud.thrift.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Connection {
	private static final Logger logger = LoggerFactory.getLogger(Connection.class);
	private static Properties properties;
	
	public static Properties loadProperties(String fileName) {
		properties = new Properties();
		if (null == fileName || fileName.isEmpty())
			throw new NullPointerException("Config name is null...");
		InputStream inputStream = null;
		try {
			inputStream = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(fileName);
			properties.load(inputStream);
			
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (properties == null)
			throw new RuntimeException("Properties file loading failed: "
					+ fileName);
		return properties;
	}
	


}
