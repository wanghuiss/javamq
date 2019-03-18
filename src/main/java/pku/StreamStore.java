package pku;

import pku.binpack.MessagePack;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wanghui on 2017/12/14.
 */

public class StreamStore {
    public static final String PATH = "./data/";

    static {
        File dir = new File(PATH);
        if(!dir.exists()){
            dir.mkdir();
        }
    }

    static final StreamStore store = new StreamStore();
    private static final Map<String, DataOutputStream> outputStreams = new ConcurrentHashMap<>();

    private StreamStore() { }

    public void init(String topic) throws Exception {

        if (!outputStreams.containsKey(topic)) {
            File file = new File(PATH + topic + ".ser");
            DataOutputStream bos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));

            outputStreams.put(topic, bos);
        }
    }

    public void write(ByteMessage msg, String topic) throws Exception {
        DataOutputStream dos = get(topic);

        byte[] bytes = MessagePack.packer.encodeMessage(msg);
        synchronized (dos) {
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }

    }

    public DataOutputStream get(String key) {

        return outputStreams.get(key);
    }

    public void flush(String topic) throws Exception {
        DataOutputStream dos = get(topic);
        dos.flush();
    }
}
