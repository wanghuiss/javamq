package pku;


import pku.binpack.MessagePack;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wanghui on 2017/12/14.
 */

public class Consumer {
    private List<String> topics = new LinkedList<>();
    private String queue;
    private int readPos;

    private Map<String, DataInputStream> inputs = new ConcurrentHashMap<>();

    public void attachQueue(String queueName, Collection<String> topics) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }

        queue = queueName;
        this.topics.addAll(topics);
        readPos = 0;

        for (String topic : topics) {
            File file = new File(Store.PATH + topic + ".ser");

            if (file.exists()) {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                inputs.put(topic, in);
            }
        }

    }

    public ByteMessage poll() throws Exception {
        ByteMessage msg = null;

        while (readPos < topics.size()) {
            String topic = topics.get(readPos);

            DataInputStream in = inputs.get(topic);

            if (in == null) {
                readPos += 1;
                continue;
            }

            try {

//                msg = assembleMessage(in);
                int length = in.readInt();
                byte[] bytes = new byte[length];
                synchronized (in) {
                    in.read(bytes);
                }
                msg = MessagePack.packer.decodeMessage(bytes);

            } catch (EOFException e) {
                readPos += 1;
                continue;
            }

            break;

        }



        return msg;

    }

    private ByteMessage assembleMessage(DataInputStream in) throws Exception {
        ByteMessage msg = null;
        synchronized (in) {
            int length = in.readInt();
            byte[] body = new byte[length];

            in.read(body);
            msg = new DefaultMessage(body);

            byte[] masks = new byte[2];
            in.read(masks);


            KeyValue kv = new DefaultKeyValue();
            for (int i = 0; i < Store.keys.length; i++) {
                byte mask = masks[i / 8], key = Store.key_masks[i % 8];

                if ((mask & key) == key) {
                    byte value_type = in.readByte();
                    if (value_type == Store.TYPE_INT) {
                        int value = in.readInt();
                        kv.put(Store.keys[i], value);

                    } else if (value_type == Store.TYPE_DOUBLE) {
                        double value = in.readDouble();
                        kv.put(Store.keys[i], value);

                    } else if (value_type == Store.TYPE_LONG) {
                        long value = in.readLong();
                        kv.put(Store.keys[i], value);

                    } else if (value_type == Store.TYPE_STRING) {
                        String value = in.readUTF();
                        kv.put(Store.keys[i], value);
                    } else if (value_type == Store.TYPE_NULL) {
                        kv.put(Store.keys[i], null);
                    }

                }
            }
            msg.setHeaders(kv);
        }

        return msg;
    }


}
