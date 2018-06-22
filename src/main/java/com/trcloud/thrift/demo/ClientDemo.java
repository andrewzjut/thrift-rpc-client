package com.trcloud.thrift.demo;

import java.util.HashMap;
import java.util.Map;

import com.trcloud.thrift.client.MesdistributeService;
import com.trcloud.thrift.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientDemo {
    private static final Logger logger = LoggerFactory.getLogger(ClientDemo.class);
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("fileDistributeNum", "4");
        map.put("messageDir", "../usr/local/filewatcher/");
//      map.put("messageDir", "/Users/zhangtong/Desktop/");
//      map.put("messageDir","/home/devops/filewatcher/");

        map.put("serverIp", "127.0.0.1");
        //请以实际ip为准，线上环境101.71.241.100  测试环境 可在本地起 kafka-rpc-server
        Config.setConfig(map);

        try {
            MesdistributeService.getInstance().sendMessage("xctest1", "哈哈");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
