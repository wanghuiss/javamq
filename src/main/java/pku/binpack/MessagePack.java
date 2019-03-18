package pku.binpack;

import pku.ByteMessage;
import pku.DefaultKeyValue;
import pku.DefaultMessage;
import pku.KeyValue;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class MessagePack {
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

    private static final byte[] key_masks = {MESSAGE_ID, TOPIC, BORN_TIMESTAMP, BORN_HOST, STORE_TIMESTAMP, STORE_HOST,
            START_TIME, STOP_TIME,
            TIMEOUT, PRIORITY, RELIABILITY, SEARCH_KEY, SCHEDULE_EXPRESSION, SHARDING_KEY,
            SHARDING_PARTITION, TRACE_ID};

    private static final String[] keys = {"MessageId", "Topic", "BornTimestamp", "BornHost", "StoreTimestamp", "StoreHost",
            "StartTime", "StopTime", "Timeout", "Priority", "Reliability", "SearchKey",
            "ScheduleExpression", "ShardingKey", "ShardingPartition", "TraceId"};

    private static final byte TYPE_INT = 0x01;
    private static final byte TYPE_DOUBLE = 0x02;
    private static final byte TYPE_LONG = 0x04;
    private static final byte TYPE_STRING = 0x08;

    public static final MessagePack packer = new MessagePack();
    private MessagePack() {}


    public byte[] encodeMessage(ByteMessage msg) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // body
        packBlob(out, msg.getBody());


        // masks
        Map kvs = msg.headers().getMap();
        byte[] masks = getMasks(kvs);
        packByte(out, masks[0]);
        packByte(out, masks[1]);


        // headers
        for (int i = 0; i < key_masks.length; i++) {
            if (kvs.containsKey(keys[i])) {
                Object object = kvs.get(keys[i]);

                if (object instanceof Integer) {
                    packInteger(out, (Integer) object, false);

                } else if (object instanceof Double) {
                    packDouble(out, (Double) object);

                } else if (object instanceof Long) {
                    packLong(out, (Long) object);

                } else if (object instanceof String) {
                    packString(out, (String) object);

                }
            }
        }

        return out.toByteArray();
//        return compress(out.toByteArray());
    }

    public ByteMessage decodeMessage(byte[] bytes) throws Exception {
        DecodeCtx ctx = new DecodeCtx();
//        ctx.buf = depress(bytes);
        ctx.buf = bytes;
        ctx.pos = 0;

        // body
        byte[] body = makeBlob(ctx);
        ByteMessage msg = new DefaultMessage(body);

        // mask
        byte[] masks = {makeByte(ctx), makeByte(ctx)};


        // headers
        KeyValue kv = new DefaultKeyValue();
        for (int i = 0; i < keys.length; i++) {
            byte mask = masks[i / 8], key = key_masks[i % 8];

            if ((mask & key) == key) {
                byte value_type = makeByte(ctx);
                if (value_type == TYPE_INT) {
                    int value = makeInteger(ctx);
                    kv.put(keys[i], value);

                } else if (value_type == TYPE_DOUBLE) {
                    double value = makeDouble(ctx);
                    kv.put(keys[i], value);

                } else if (value_type == TYPE_LONG) {
                    long value = makeLong(ctx);
                    kv.put(keys[i], value);

                } else if (value_type == TYPE_STRING) {
                    String value = makeString(ctx);
                    kv.put(keys[i], value);
                }

            }
        }

        msg.setHeaders(kv);


        return msg;
    }

    private byte[] compress(byte[] data) throws Exception {

        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.setLevel(Deflater.BEST_COMPRESSION);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        deflater.finish();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }

        outputStream.close();

        return outputStream.toByteArray();
    }

    private byte[] depress(byte[] data) throws Exception {

        Inflater inflater = new Inflater();
        inflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();

        return outputStream.toByteArray();
    }

    private Integer makeInteger(DecodeCtx ctx) {
        int x = 0;
        byte shift = 0;
        int n;
        while (shift < 32) {
            n = ctx.buf[ctx.pos++];
            x |= (n & 0xff) << shift;
            shift += 8;
        }

        return x;
    }

    private Double makeDouble(DecodeCtx ctx) {
        long x = 0;
        byte shift = 0;
        long n;
        while (shift < 64) {
            n = (long) ctx.buf[ctx.pos++];
            x |= (n & 0xff) << shift;
            shift += 8;
        }
        return Double.longBitsToDouble(x);
    }

    private Long makeLong(DecodeCtx ctx) {
        long x = 0;
        byte shift = 0;
        long n;
        while (shift < 64) {
            n = (long) ctx.buf[ctx.pos++];
            x |= (n & 0xff) << shift;
            shift += 8;
        }
        return x;
    }

    private String makeString(DecodeCtx ctx) {
        int len = makeInteger(ctx);

        String s = new String(ctx.buf, ctx.pos, len);
        ctx.pos += len;

        return s;
    }

    private byte[] makeBlob(DecodeCtx ctx) {
        int len = makeInteger(ctx);

        byte[] bytes = copyBytes(ctx.buf, ctx.pos, len);
        ctx.pos += len;

        return bytes;
    }

    private byte makeByte(DecodeCtx ctx) {
        return ctx.buf[ctx.pos++];
    }


    private byte[] getMasks(Map kvs) {
        byte[] masks = {0x00, 0x00};

        for (int i = 0; i < keys.length; i++) {
            if (kvs.containsKey(keys[i])) {
                byte type = key_masks[i % 8];
                masks[i / 8] |= type;
            }
        }

        return masks;
    }


    private void packInteger(OutputStream out, Integer i, boolean isLength) throws IOException {
        if (!isLength) out.write(TYPE_INT);

        // high bits
        byte shift = 32;
        while (shift > 0) {
            out.write((byte) (i & 0xff));
            i = i >> 8;
            shift -= 8;
        }
    }

    private void packDouble(OutputStream out, Double d) throws IOException {
        long x = Double.doubleToLongBits(d);
        out.write(TYPE_DOUBLE);

        byte shift = 64;

        while (shift > 0) {
            out.write((byte) (x & 0xff));
            x = x >> 8;
            shift -= 8;
        }
    }

    private void packLong(OutputStream out, Long l) throws IOException {
        out.write(TYPE_LONG);

        byte shift = 64;

        while (shift > 0) {
            out.write((byte) (l & 0xff));
            l = l >> 8;
            shift -= 8;
        }
    }

    private void packString(OutputStream out, String s) throws IOException {
        out.write(TYPE_STRING);

        packInteger(out, s.length(), true);
        out.write(s.getBytes());

    }


    private void packBlob(OutputStream out, byte[] bytes) throws IOException {
        packInteger(out, bytes.length, true);
        out.write(bytes);
    }

    private void packByte(OutputStream out, byte b) throws IOException {
        out.write(b);
    }


    private byte[] copyBytes(byte[] bytes, int start, int length) {
        byte[] copy = new byte[length];
        for (int i = 0; i < length; i++) {
            copy[i] = bytes[i + start];
        }
        return copy;
    }

    private static class DecodeCtx {
        byte[] buf;
        int pos;
    }


}
