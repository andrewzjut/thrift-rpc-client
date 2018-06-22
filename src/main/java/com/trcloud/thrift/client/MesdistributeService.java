package com.trcloud.thrift.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.trcloud.thrift.util.Config;
import com.trcloud.thrift.util.DateUtil;
import com.trcloud.thrift.util.KafkaRecord;
import com.trcloud.thrift.util.StreamingUtil;

/**
 * 
 * @author BenSNW
 *
 */
public class MesdistributeService {
	private static final Logger logger = LoggerFactory
			.getLogger(MesdistributeService.class);
	private static String messageDir = Config.messageDir;
	private static MesdistributeService messagedistribute = null;
	private static int distributeNum = Config.distributeNum;
	private static int refreshSec = Config.interval;
	private static String suffix = Config.suffix;
	private static String nowDate;
	private static Map<Integer, BufferedWriter> map = new HashMap<Integer, BufferedWriter>();
	private static KafkaRecord record = new KafkaRecord();
	static {

		try {
			logger.info("------------monitor  start-------------------");
			FileMonitor m = new FileMonitor(refreshSec);
			File path = new File(messageDir);
			if (!path.exists()) {
				path.mkdirs();
			}
			m.monitor(messageDir, new FileListener());
			m.start();
			logger.info("------------monitor start success------------");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("-----------monitor start fail---------------");
		}

	}

	private MesdistributeService() {
	}

	public static synchronized MesdistributeService getInstance() {
		if (messagedistribute == null) {
			messagedistribute = new MesdistributeService();
			nowDate = DateUtil.getNowdate();
			for (int i = 0; i < distributeNum; i++) {
				map.put(i, getBufferedWriter(messageDir + "/" + nowDate + "--"
						+ i + suffix));
			}
		}
		return messagedistribute;
	}

	/**
	 *
	 * @param topic kafka topic
	 * @param message 消息内容
	 * @return
	 * @throws Exception
	 */
	public synchronized boolean sendMessage(String topic, String message)
			throws Exception {
		if (StringUtils.isNotBlank(message)) {
			try {
				String newDate = DateUtil.getNowdate();
				if (!nowDate.equals(newDate)) {
					nowDate = newDate;
					for (Integer key : map.keySet()) {
						map.get(key).close();
						map.put(key, getBufferedWriter(messageDir + "/" + "/"
								+ nowDate + "--" + key + suffix));
					}
					record.setTopic(topic);
					record.setValue(StreamingUtil.byte2String(message
							.getBytes()));
					String jsonString = JSON.toJSONString(record);
					write(map, jsonString);
				} else {
					record.setTopic(topic);
					record.setValue(StreamingUtil.byte2String(message
							.getBytes()));
					String jsonString = JSON.toJSONString(record);
					write(map, jsonString);
				}
				return true;
			} catch (Exception e) {
				logger.error(e.getMessage());
				return false;
			}
		} else {
			return false;
		}

	}

	public static BufferedWriter getBufferedWriter(String path) {
		String dir = path.substring(0, path.lastIndexOf("/"));
		File fileDir = new File(dir);
		File file = new File(path);
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			if (!fileDir.exists()) {
				fileDir.mkdirs();
			}
			if (!file.exists()) {
				file.createNewFile();
			}
			fw = new FileWriter(file, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		return bw;
	}

	public static void write(Map<Integer, BufferedWriter> map, String message) {
		int hash = Math.abs(UUID.randomUUID().hashCode()) % distributeNum;
		try {
			map.get(hash).write(message + "\r\n");
			map.get(hash).flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public synchronized boolean sendMessage(String topic, Object message)
			throws IOException {
		try {
			String newDate = DateUtil.getNowdate();
			if (!nowDate.equals(newDate)) {
				nowDate = newDate;
				for (Integer key : map.keySet()) {
					map.get(key).close();
					map.put(key, getBufferedWriter(messageDir + "/" + "/"
							+ nowDate + "--" + key + suffix));
				}
				record.setTopic(topic);
				byte[] obj2AvroBytes = StreamingUtil.obj2AvroBytes(message);
				String str = StreamingUtil.byte2String(obj2AvroBytes);
				record.setValue(str);
				String jsonString = JSON.toJSONString(record);
				write(map, jsonString);
			} else {
				record.setTopic(topic);
				byte[] obj2AvroBytes = StreamingUtil.obj2AvroBytes(message);
				String str = StreamingUtil.byte2String(obj2AvroBytes);
				record.setValue(str);
				String jsonString = JSON.toJSONString(record);
				write(map, jsonString);
			}
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

	}

}
