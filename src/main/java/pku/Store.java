package pku;

import pku.binpack.MessagePack;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Store {
    public static final String PATH = "./data/";

    private static final byte MESSAGE_ID = 0x01; // 0000 0001
    private static final byte TOPIC = 0x02; // 0000 0010
    private static final byte BORN_TIMESTAMP = 0x04; // 0000 0100
    private static final byte BORN_HOST = 0x08; // 0000 1000
    private static final byte STORE_TIMESTAMP = 0x10; // 0001 0000
    private static final byte STORE_HOST = 0x20; // 0010 0000
    private static final byte START_TIME = 0x40; // 0100 0000
    private static final byte STOP_TIME = (byte) 0x80; // 1000 0000
    private static final byte TIMEOUT = 0x01; // 0000 0001
    private static final byte PRIORITY = 0x02; // 0000 0010
    private static final byte RELIABILITY = 0x04; // 0000 0100
    private static final byte SEARCH_KEY = 0x08; // 0000 1000
    private static final byte SCHEDULE_EXPRESSION = 0x10; // 0001 0000
    private static final byte SHARDING_KEY = 0x20; // 0010 0000
    private static final byte SHARDING_PARTITION = 0x40; // 0100 0000
    private static final byte TRACE_ID = (byte) 0x80; // 1000 0000

    public static final byte[] key_masks = {MESSAGE_ID, TOPIC, BORN_TIMESTAMP, BORN_HOST, STORE_TIMESTAMP,  STORE_HOST,
            START_TIME, STOP_TIME,
            TIMEOUT, PRIORITY, RELIABILITY, SEARCH_KEY, SCHEDULE_EXPRESSION, SHARDING_KEY,
            SHARDING_PARTITION, TRACE_ID};

    public static final String[] keys = {"MessageId", "Topic", "BornTimestamp", "BornHost", "StoreTimestamp", "StoreHost",
            "StartTime", "StopTime", "Timeout", "Priority", "Reliability", "SearchKey",
            "ScheduleExpression", "ShardingKey", "ShardingPartition", "TraceId"};

    public static final byte TYPE_INT = 0x01;
    public static final byte TYPE_DOUBLE = 0x02;
    public static final byte TYPE_LONG = 0x04;
    public static final byte TYPE_STRING = 0x08;
    public static final byte TYPE_NULL = 0x0f; // 0000 1111

    static {
        File dir = new File(PATH);
        if(!dir.exists()){
            dir.mkdir();
        }
    }

    static final Store store = new Store();
    private static final Map<String, DataOutputStream> outputStreams = new ConcurrentHashMap<>();

    private Store() { }

    public void init(String topic) throws Exception {

        if (!outputStreams.containsKey(topic)) {
            File file = new File(PATH + topic + ".ser");
            DataOutputStream bos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));

            outputStreams.put(topic, bos);
        }
    }

    public void write(ByteMessage msg, String topic) throws Exception {

        DataOutputStream dos = get(topic);

        synchronized (dos) {

            // write body length and body bytes
            dos.writeInt(msg.getBody().length);
            dos.write(msg.getBody());

            // write masks and headers
            Map kvs = msg.headers().getMap();
            dos.write(getMasks(kvs));

            for (int i = 0; i < key_masks.length; i++) {
                if (kvs.containsKey(keys[i])) {
                    Object object = kvs.get(keys[i]);

                    if (object instanceof Integer) {
                        dos.writeByte(TYPE_INT);
                        dos.writeInt((Integer) object);

                    } else if (object instanceof Double) {
                        dos.writeByte(TYPE_DOUBLE);
                        dos.writeDouble((Double) object);

                    } else if (object instanceof Long) {
                        dos.writeByte(TYPE_LONG);
                        dos.writeLong((Long) object);

                    } else if (object instanceof String) {
                        dos.writeByte(TYPE_STRING);
                        dos.writeUTF((String) object);

                    } else if (object == null) {
                        dos.writeByte(TYPE_NULL);
                    }
                }
            }
        }

    }

    public DataOutputStream get(String key) {

        return outputStreams.get(key);
    }

    public void flush(String topic) throws Exception {
        DataOutputStream dos = get(topic);
        dos.flush();
    }

    private byte[] getMasks(Map kvs) {
        byte[] masks = {0x00, 0x00};

        for (int i = 0; i < keys.length; i++) {
            if (kvs.containsKey(keys[i])){
                byte type = key_masks[i % 8];
                masks[i / 8] |= type;
            }
        }

        return masks;
    }


}
