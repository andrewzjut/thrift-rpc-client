package com.trcloud.thrift.util;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class Config {
	private static Config config;
	private static Properties p ;
	public static int distributeNum = 1;
	public static int  batch = 10;
	public static int  interval = 10000;
	public static String messageDir = "/RPC/tmp/data";
	public static int retentionDays = 7;
	public static String suffix = ".message";
	public static long waitMessageTime = 1000;
	public static long reconnect_time = 3000;
	public static String server_ip = "127.0.0.1";
	public static int server_port = 8888;
	public static String charset = "utf-8";
	public static String dateFormat = "yyyy-MM-dd";
	private static final Logger logger = LoggerFactory.getLogger(Config.class);

	static{
		if(null!=Config.class.getClassLoader().getResource("rpcclient.properties")){
			p= Connection.loadProperties("rpcclient.properties");
			getValue(p);
		}
		logger.warn("默认配置 distributeNum={},batch={},interval={},messageDir={},suffix={}," +
				"waitMessageTime={},reconnect_time={},server_ip={},server_port={},charset={},dateFormat={}",
				distributeNum,batch,interval,messageDir,suffix,waitMessageTime,reconnect_time,server_ip,server_port,charset,dateFormat);
	}
	
	public synchronized static void setConfig(Map<String, String> map){
		if(null ==config){
			config = new Config();
			p = new Properties();
			p.putAll(map);
			getValue(p);
		}
		logger.warn("用户定义配置 distributeNum={},batch={},interval={},messageDir={},suffix={}," +
						"waitMessageTime={},reconnect_time={},server_ip={},server_port={},charset={},dateFormat={}",
				distributeNum,batch,interval,messageDir,suffix,waitMessageTime,reconnect_time,server_ip,server_port,charset,dateFormat);
	}

	private static void getValue(Properties p ){
		String num = String.valueOf(p.get("fileDistributeNum"));
		String monitor = String.valueOf(p.get("monitorInterval"));
		String reDays = String.valueOf(p.get("messageRetentionDays"));
		String dir = String.valueOf(p.get("messageDir"));
		 String suf= String.valueOf(p.get("messageSuffix"));//
		 String ip = String.valueOf(p.get("serverIp"));
		 String crset = String.valueOf(p.get("charset"));
		 String format = String.valueOf(p.get("dateFormat"));
		 String port = String.valueOf(p.get("serverPort"));
		 String batchSize = String.valueOf(p.get("batchMessageSize"));
		String wm =  String.valueOf(p.get("waitMessageInterval"));
		String reconnect = String.valueOf(p.get("reconnectTime"));
		if(!num.equals("null"))
			distributeNum = Integer.parseInt(num);
		if(!monitor.equals("null"))
			interval = Integer.parseInt(monitor);
		if(!reDays.equals("null"))
			retentionDays = Integer.parseInt(reDays);
		if(!dir.equals("null"))
			messageDir = dir;
		if(!suf.equals("null"))
			suffix = suf;
		if(!ip.equals("null"))
			server_ip = ip;
		if(!crset.equals("null"))
			charset = crset;
		if(!format.equals("null"))
			dateFormat = format;
		if(!port.equals("null"))
			server_port =Integer.parseInt( port);
		if(!batchSize.equals("null")) 
			batch = Integer.parseInt(batchSize);
		if(!wm.equals("null"))
			 waitMessageTime = Long.parseLong(wm);
		if(!reconnect.equals("null"))
			reconnect_time = Long.parseLong(reconnect);
	}
	public static Properties getP() {
		return p;
	}
	public static void setP(Properties p) {
		Config.p = p;
	}
	public static int getDistributeNum() {
		return distributeNum;
	}
	public static void setDistributeNum(int distributeNum) {
		Config.distributeNum = distributeNum;
	}
	public static int getBatch() {
		return batch;
	}
	public static void setBatch(int batch) {
		Config.batch = batch;
	}
	public static int getInterval() {
		return interval;
	}
	public static void setInterval(int interval) {
		Config.interval = interval;
	}
	public static String getMessageDir() {
		return messageDir;
	}
	public static void setMessageDir(String messageDir) {
		Config.messageDir = messageDir;
	}
	public static int getRetentionDays() {
		return retentionDays;
	}
	public static void setRetentionDays(int retentionDays) {
		Config.retentionDays = retentionDays;
	}
	public static String getSuffix() {
		return suffix;
	}
	public static void setSuffix(String suffix) {
		Config.suffix = suffix;
	}
	public static long getWaitMessageTime() {
		return waitMessageTime;
	}
	public static void setWaitMessageTime(long waitMessageTime) {
		Config.waitMessageTime = waitMessageTime;
	}
	public static long getReconnect_time() {
		return reconnect_time;
	}
	public static void setReconnect_time(long reconnect_time) {
		Config.reconnect_time = reconnect_time;
	}
	public static String getServer_ip() {
		return server_ip;
	}
	public static void setServer_ip(String server_ip) {
		Config.server_ip = server_ip;
	}
	public static int getServer_port() {
		return server_port;
	}
	public static void setServer_port(int server_port) {
		Config.server_port = server_port;
	}
	public static String getCharset() {
		return charset;
	}
	public static void setCharset(String charset) {
		Config.charset = charset;
	}
	public static String getDateFormat() {
		return dateFormat;
	}
	public static void setDateFormat(String dateFormat) {
		Config.dateFormat = dateFormat;
	}
	
	
	
}
