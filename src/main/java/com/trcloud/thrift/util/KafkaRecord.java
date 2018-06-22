package com.trcloud.thrift.util;

/**
 * Created by hzzt on 2016/9/29.
 */
public class KafkaRecord {
	private String topic;
	private String value;

	public KafkaRecord() {
	}

	public KafkaRecord(String topic, String value) {
		this.topic = topic;
		this.value = value;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}


	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
