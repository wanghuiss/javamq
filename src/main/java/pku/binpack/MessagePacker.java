package pku.binpack;

import pku.ByteMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class MessagePacker {
    private static final String ENCODING = "UTF-8";
    private static final int LENGTH_BYTES = 4;

    public static byte[] encodeMessage(ByteMessage msg) throws Exception{
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] bytes = Packer.encode(msg, ENCODING);

        out.write(int2bytes(bytes.length)); // write message header length
        out.write(bytes);

        return out.toByteArray();
    }

    public static ByteMessage decodeMessage(InputStream is) throws Exception{

        // get byte length
        int length = bytes2int(readBytes(is, LENGTH_BYTES));
        if (length == 0) return null;

        byte[] bytes = readBytes(is, length);

        ByteMessage msg = (ByteMessage) Packer.decode(bytes, ENCODING);

        return msg;
    }

    public static ByteMessage decodeMessage(MappedByteBuffer buffer) throws Exception {
        byte[] length = new byte[4];
        buffer.get(length);

        int len = bytes2int(length);
        byte[] bytes = new byte[len];
        buffer.get(bytes);

        ByteMessage msg = (ByteMessage) Packer.decode(bytes, ENCODING);

        return msg;
    }

    private static byte[] compress(byte[] data) {
        byte[] output;

        Deflater compresser = new Deflater();

        compresser.reset();
        compresser.setInput(data);
        compresser.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!compresser.finished()) {
                int i = compresser.deflate(buf);
                bos.write(buf, 0, i);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            output = data;
        }
        compresser.end();
        return output;
    }

    public static byte[] decompress(byte[] data) {
        byte[] output;

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data);

        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!decompresser.finished()) {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e) {
            output = data;
            e.printStackTrace();
        }

        decompresser.end();
        return output;
    }

    private static byte[] readBytes(InputStream is, int length) throws Exception{
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] buffer = new byte[1024];
        int read = 0;

        while (read < length) {
            int cur = is.read(buffer, 0, Math.min(1024, length-read));

            if (cur < 0) break;
            read += cur;
            out.write(buffer, 0, cur);
        }

        return out.toByteArray();
    }

    private static byte[] int2bytes(int num){
        byte[] result = new byte[4];
        result[0] = (byte)((num >>> 24) & 0xff);
        result[1] = (byte)((num >>> 16)& 0xff );
        result[2] = (byte)((num >>> 8) & 0xff );
        result[3] = (byte)((num >>> 0) & 0xff );
        return result;
    }

    private static int bytes2int(byte[] bytes){
        int result = 0;
        if(bytes.length == 4){
            int a = (bytes[0] & 0xff) << 24;
            int b = (bytes[1] & 0xff) << 16;
            int c = (bytes[2] & 0xff) << 8;
            int d = (bytes[3] & 0xff);
            result = a | b | c | d;
        }
        return result;
    }

}
