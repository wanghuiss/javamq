package pku.binpack;

import pku.ByteMessage;
import pku.DefaultKeyValue;
import pku.DefaultMessage;
import pku.KeyValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class Packer {

    public static final byte TAG_SHUT = 0x01; // 0000 0001
    public static final byte TYPE_MESSAGE = 0x02; // 0000 0010
    public static final byte TYPE_DICT = 0x03; // 0000 0011

    public static final byte TYPE_BOOL = 0x04; // 0000 0100
    public static final byte TYPE_BOOL_FALSE = 0x05; // 0000 0101

    public static final byte TYPE_REAL_DOUBLE = 0x06; // 0000 0110
    public static final byte TYPE_REAL_FLOAT = 0x07; // 0000 0111

    public static final byte TYPE_NULL = 0x0f; // 0000 1111

    public static final byte TYPE_BLOB = 0x10; // 0001 0000
    public static final byte TYPE_STRING = 0x20; // 0010 0000

    public static final byte TAG_PACK_NUM = 0x0f; // 0001 xxxx

    public static final byte TAG_PACK_INTEGER = 0x07; // 0000 0xxx

    public static final byte TYPE_INTEGER = 0x40; // 0100 0000
    public static final byte TYPE_INTEGER_NEGATIVE_MASK = 0x20; // 0010 0000

    public static final byte INTEGER_TYPE_Byte = 0x01 << 3; // xxx0 1xxx
    public static final byte INTEGER_TYPE_Short = 0x02 << 3; // xxx1 0xxx
    public static final byte INTEGER_TYPE_Int = 0x03 << 3; // xxx1 1xxx
    public static final byte INTEGER_TYPE_Long = 0x00 << 3; // xxx0 0xxx
    public static final byte INTEGER_TYPE_MASK = 0x18; // 0001 1000

    public static final byte NUM_SIGN_BIT = (byte) 0x80; // 1000 0000
    public static final byte NUM_MASK = (byte) 0x7f; // 1000 0000

    public static final Object SHUT_OBJECT = new Object();

    /**
     * Encode data into byte array
     *
     * @param obj
     * @param charsetName
     * @return byte[]
     */
    public static byte[] encode(Object obj, String charsetName) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            pack(out, obj, charsetName);
            return out.toByteArray();
        } catch (IOException ex) {
            byte[] bs = {};
            return bs;
        }
    }

    public static void pack(OutputStream out, Object obj, String charsetName) throws IOException {
        if (obj == null) {
            packNull(out);
        } else if (obj instanceof Integer || obj instanceof Long || obj instanceof Short || obj instanceof Byte) {
            packInteger(out, (Number) obj);
        } else if (obj instanceof String) {
            packString(out, obj.toString(), charsetName);
        } else if (obj instanceof Boolean) {
            packBool(out, ((Boolean) obj).booleanValue());
        } else if (obj instanceof byte[]) {
            packBlob(out, (byte[]) obj);
        } else if (obj instanceof Double) {
            packDouble(out, ((Double) obj));
        } else if (obj instanceof Float) {
            packFloat(out, ((Float) obj));
        } else if (obj instanceof ByteMessage) {
            packMessage(out, (ByteMessage) obj, charsetName);
        } else if (obj instanceof Map) {
            packMap(out, (Map) obj, charsetName);
        } else {
            packString(out, "unsupported-type-" + obj.getClass().getName(), "UTF-8");
        }
    }

    /**
     * Decode from byte array
     *
     * @param bs
     * @param charsetName
     * @return
     */
    public static Object decode(byte[] bs, String charsetName) {
        Packer.DecodeCtx ctx = new Packer.DecodeCtx();
        ctx.buf = bs;
        ctx.pos = 0;
        ctx.charsetName = charsetName;
        Object obj = doDecode(ctx);
        if (obj == SHUT_OBJECT) {
            return null;
        }
        return obj;
    }

    private static int unpackTag(Packer.DecodeCtx ctx, Packer.TagInfo info) {
        if (ctx.pos >= ctx.buf.length) {
            return -2;
        }
        long x = (long) ctx.buf[ctx.pos++];

        int shift = 0;
        long num = 0;
        if (x < 0) {
            while (x < 0) {
                x &= 0x7f;
                num |= x << shift;
                x = (long) ctx.buf[ctx.pos++];
                shift += 7;
            }
        }

        // not length information
        byte type = (byte) x;
        if (type < 0x10) {

            info.type = type;
        } else {

            if (type < TYPE_INTEGER) {
                info.type = (byte) (type & 0x70);
                num |= (x & 0x0f) << shift;
            } else {
                info.type = type;
                num |= (x & 0x07) << shift;
            }

            info.num = num;
        }

        return 0;
    }

    private static Object doDecode(Packer.DecodeCtx ctx) {
        Packer.TagInfo info = new Packer.TagInfo();
        if (unpackTag(ctx, info) < 0) {
            return null;
        }

        if (info.type >= TYPE_INTEGER) {
            return makeInteger(info);
        }

        switch (info.type) {

            case TAG_SHUT:
                return SHUT_OBJECT;

            case TYPE_MESSAGE: {
                ByteMessage msg = new DefaultMessage();
                makeMessage(ctx, msg);

                return msg;
            }

            case TYPE_DICT: {
                Map map = new HashMap();
                if (makeDict(ctx, map) < 0) {
                    return null;
                }
                return map;
            }

            case TYPE_BOOL:
                return true;
            case TYPE_BOOL_FALSE:
                return false;

            case TYPE_NULL:
                return null;

            case TYPE_BLOB: {
                if (info.num > ctx.buf.length - ctx.pos) {
                    return null;
                }
                int start = ctx.pos;
                ctx.pos += info.num;
                return Arrays.copyOfRange(ctx.buf, start, ctx.pos);
            }

            case TYPE_STRING: {
                int start = ctx.pos;
                ctx.pos += info.num;
                try {
                    return new String(ctx.buf, start, (int) info.num, ctx.charsetName);
                } catch (Exception ex) {
                    return null;
                }
            }
            case TYPE_REAL_DOUBLE: {
                return makeDouble(ctx);
            }
            case TYPE_REAL_FLOAT: {
                return makeFloat(ctx);
            }
        }

        return null;
    }

    private static int makeMessage(Packer.DecodeCtx ctx, ByteMessage msg) {
        HashMap<String, Object> map = (HashMap<String, Object>) doDecode(ctx);
        KeyValue kvs = new DefaultKeyValue(map);
        msg.setHeaders(kvs);

        msg.setBody((byte[]) doDecode(ctx));

        return 0;
    }

    private static int makeDict(Packer.DecodeCtx ctx, Map map) {
        while (true) {
            Object key = doDecode(ctx);
            if (key == SHUT_OBJECT) {
                return 0;
            }

            if (key == null) {
                return -1;
            }

            Object value = doDecode(ctx);
            if (value == SHUT_OBJECT) {
                return -1;
            }

            map.put(key, value);
        }
    }

    private static Object makeInteger(Packer.TagInfo info) {
        int type = info.type & INTEGER_TYPE_MASK;
        if ((info.type & TYPE_INTEGER_NEGATIVE_MASK) != 0) {
            info.num = -info.num;
        }
        switch (type) {
            case INTEGER_TYPE_Byte:
                return (byte) info.num;
            case INTEGER_TYPE_Short:
                return (short) info.num;
            case INTEGER_TYPE_Int:
                return (int) info.num;
            case INTEGER_TYPE_Long:
                return info.num;
            default:
                break;
        }
        return null;
    }

    private static Double makeDouble(Packer.DecodeCtx ctx) {

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

    private static Float makeFloat(Packer.DecodeCtx ctx) {

        int x = 0;
        byte shift = 0;
        int n;
        while (shift < 32) {
            n = (int) ctx.buf[ctx.pos++];
            x |= (n & 0xff) << shift;
            shift += 8;
        }
        return Float.intBitsToFloat(x);
    }

    private static void packDouble(OutputStream out, Double d) throws IOException {
        long x = Double.doubleToLongBits(d);
        out.write(TYPE_REAL_DOUBLE);

        byte shift = 64;

        while (shift > 0) {
            out.write((byte) (x & 0xff));
            x = x >> 8;
            shift -= 8;
        }
    }

    private static void packFloat(OutputStream out, Float f) throws IOException {
        int x = Float.floatToIntBits(f);
        out.write(TYPE_REAL_FLOAT);

        byte shift = 32;
        while (shift > 0) {
            out.write((byte) (x & 0xff));
            x = x >> 8;
            shift -= 8;
        }
    }

    private static void packMessage(OutputStream out, ByteMessage msg, String charsetName) throws IOException {
        out.write(TYPE_MESSAGE);
        packMap(out, msg.headers().getMap(), charsetName);
        packBlob(out, msg.getBody());

    }

    private static void packMap(OutputStream out, Map map, String charsetName) throws IOException {
        out.write(TYPE_DICT);
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            pack(out, entry.getKey(), charsetName);
            pack(out, entry.getValue(), charsetName);
        }
        out.write(TAG_SHUT);
    }

    private static void packString(OutputStream out, String s, String charsetName) throws IOException {
        byte[] bs = s.getBytes(charsetName);
        packNum(out, bs.length, TYPE_STRING);
        out.write(bs);
    }

    private static void packNum(OutputStream out, int len, byte type) throws IOException {
        // last number byte: 0000 xxxx
        while (len > TAG_PACK_NUM) {
            out.write((byte) (NUM_SIGN_BIT | (len & NUM_MASK)));
            len = len >>> 7;
        }
        out.write(type | len);
    }

    private static void packBlob(OutputStream out, byte[] bs) throws IOException {
        packNum(out, bs.length, TYPE_BLOB);
        out.write(bs);
    }

    private static void packNull(OutputStream out) throws IOException {
        out.write(TYPE_NULL);
    }

    public static void packBool(OutputStream out, boolean v) throws IOException {
        out.write((v ? TYPE_BOOL : TYPE_BOOL_FALSE));
    }

    private static void packInteger(OutputStream out, Number n) throws IOException {
        byte tag = TYPE_INTEGER;
        if (n instanceof Byte) {
            tag |= INTEGER_TYPE_Byte;
        } else if (n instanceof Short) {
            tag |= INTEGER_TYPE_Short;
        } else if (n instanceof Integer) {
            tag |= INTEGER_TYPE_Int;
        } else if (n instanceof Long) {
            tag |= INTEGER_TYPE_Long;
        }
        long l = n.longValue();
        if (l < 0) {
            l = -l;
            tag |= TYPE_INTEGER_NEGATIVE_MASK;
        }

        // last number byte: 0000 0xxx
        while (l > TAG_PACK_INTEGER || l >>> 3 > 0) {
            out.write((byte) (NUM_SIGN_BIT | (l & NUM_MASK)));
            l = l >>> 7;
        }
        out.write((byte) (tag | l));
    }

    private static class DecodeCtx {
        byte[] buf;
        int pos;
        String charsetName;
    }

    private static class TagInfo {
        byte type;
        long num;
    }
}