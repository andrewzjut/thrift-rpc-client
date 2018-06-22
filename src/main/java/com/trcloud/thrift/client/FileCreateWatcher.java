package com.trcloud.thrift.client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;

import com.trcloud.thrift.util.*;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.trcloud.thrift.service.KafkaService.Client;
import com.trcloud.thrift.service.Result;
import com.trcloud.thrift.service.Status;


public class FileCreateWatcher implements Runnable {
    //	public static final String SERVER_IP = "172.30.251.229";
    public static final String SERVER_IP = Config.server_ip;
    //public static final String SERVER_IP = "192.168.16.67";
    public static final int SERVER_PORT = Config.server_port;
    private static long waitMessageTime = Config.waitMessageTime;
    private static long reconnect_time = Config.reconnect_time;
    private static long batchSize = Config.batch;
    private static final Logger logger = LoggerFactory.getLogger(FileCreateWatcher.class);
    private File offsetFile;
    private File logFile;
    private String offsetFileName;
    private String filePath;
    private String charset = Config.charset;

    public FileCreateWatcher(String filePath) {
        this.filePath = filePath;
        this.logFile = new File(filePath);
        this.offsetFileName = filePath + ".offset";
        this.offsetFile = new File(offsetFileName);
    }

    public FileCreateWatcher() {
    }

    private static void openTransport(TTransport transport) {
        while (true) {
            try {
                transport.open();
                logger.info("------connect   to   server   success--------");
                break;
            } catch (TTransportException e) {
                //e.printStackTrace();
                logger.error("-----connect to server fail,reconnect--------");
                try {
                    Thread.sleep(reconnect_time);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    private static void writeOffset(File offsetFile, long offset) {
        try {
            if (!offsetFile.exists()) {
                offsetFile.createNewFile();
            }
            Writer txtWriter = new FileWriter(offsetFile, false);
            txtWriter.write(offset + "");
            txtWriter.flush();
            txtWriter.close();
        } catch (IOException e) {
            logger.error("------------" + e.getMessage());
        }
    }

    @Override
    public void run() {
        logger.info("------start monitor file:" + filePath);
        TTransport transport = null;
        try {
            transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT)); //nonBlocking server
            TProtocol protocol = new TCompactProtocol(transport); //nonBlocking server
            Client client = new Client(protocol);
            openTransport(transport);

            long offset = 0;
            offset = FileUtils.getOffset(offsetFileName);
            RandomAccessFile f = new RandomAccessFile(filePath, "rw");
            int i = 0;        //messagenum
            String tmp = "";
            boolean flag = false;
            while (true) {
                int ln = 0;
                f.seek(offset);
                while ((tmp = f.readLine()) != null) {
                    Result res = null;
                    KafkaRecord parseObject = null;
                    String topic= "";
                    String value="";
                    try {
                        parseObject = JSON.parseObject(new String(tmp.trim().getBytes("ISO-8859-1"), charset), KafkaRecord.class);
                        topic = parseObject.getTopic();
                        value = parseObject.getValue();
                        logger.info("#RPC# "+new String(StreamingUtil.String2Byte(value),charset));
                    } catch (Exception e1) {
                        logger.error("-----parseJson failed,maybe the offset not true");
                        offset += tmp.getBytes("ISO-8859-1").length + 2;
                        flag = true;
                        break;
                    }

                    try {
                        if(value.getBytes().length<1024*10*1024){
                            res = client.sendMessage(topic, value);
                        }else{
                            logger.info("#RPC# "+new String(StreamingUtil.String2Byte(value),charset));
                        }

                    } catch (Exception e) {
                            transport.close();
                            openTransport(transport);
                        try {
                            res = client.sendMessage(topic, value);
                        } catch (Exception e1) {
                            logger.error("------------kafka service stoped ---------");
                            Thread.sleep(60000);
                            break;
                        }
                    }

                    if (null != res && res.status == Status.SUCCESS) {

                        offset += tmp.getBytes("ISO-8859-1").length + 2;
                        flag = true;
                        ln++;
                        i++;
                        if (ln > batchSize) {
                            writeOffset(offsetFile, offset);
                            flag = false;
                            ln = 0;
                        }
                    } else {
                        logger.error("server send kafka faild,please see kafka or server");
                        break;
                    }
                }

                if (flag) {
                    writeOffset(offsetFile, offset);
                }
                if (!offsetFile.getName().startsWith(DateUtil.getNowdate())) {    //file read off
                    if (FileUtils.isReadOff(logFile)) {
                        break;
                    }
                }
                Thread.sleep(waitMessageTime);
            }
            f.close();
            transport.close();
            logger.info(logFile.getName() + "  write to kafka total : " + i + "  message" + "---");
        } catch (Exception e) {
            logger.error("--------" + e.getMessage());
        }
    }

}

