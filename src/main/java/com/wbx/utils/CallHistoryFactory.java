package com.wbx.utils;

import com.wbx.entity.CallHistory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CallHistoryFactory {

    private static final Random random = new Random();

    private static Map<String, String> impiLocation= new HashMap<>(10000);

    private static final String[] locations = new String[]{"HENAN","BEIJING","GUANGDONG","SHANGHAI","CHONGQING","SICHUAN","HEBEI","GUANGXI","HAINAN","TIANJIN","ZHEJIANG","JIANGSU","SHANDONG"};

    private static long no = 13000000000L;

    static {
        for (int i = 0; i < 10000; i++){
            String location = locations[random.nextInt(13)];
            String impi = String.valueOf(no + random.nextInt(1999999999));
            impiLocation.put(impi, location);
        }
    }

    private CallHistoryFactory(){}

    public static CallHistory build(){
        CallHistory ch = new CallHistory();
        String[] keys = impiLocation.keySet().toArray(new String[10000]);
        String impi = keys[random.nextInt(keys.length)];
        String location = impiLocation.get(impi);
        ch.setImpiFrom(impi);
        ch.setImpiTo(keys[random.nextInt(keys.length)]);
        ch.setCallDuration(random.nextInt(1000));
        ch.setCallTime(System.currentTimeMillis() + random.nextInt(30 * 24 * 60 * 60) * 1000L);
        ch.setImpiFromLocation(location);
        return ch;
    }

    public static void main(String[] args) {
        CallHistory ch = build();
        String impi = ch.getImpiFrom();
    }

}
