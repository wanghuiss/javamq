package pku;

import java.io.Serializable;

/**
 * Created by wanghui on 2017/12/14.
 *
 */
public class DefaultMessage implements ByteMessage, Serializable{

    private static final long serialVersionUID = -1;

    private KeyValue headers = new DefaultKeyValue();
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public DefaultMessage(byte[] body) {
        this.body = body;
    }

    public DefaultMessage() { }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public KeyValue headers() {
        return headers;
    }


    public DefaultMessage putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    public DefaultMessage putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (String key : headers.keySet()) {
            sb.append(key);
            sb.append(": ");
            sb.append(headers.getObj(key).toString());
            sb.append(", ");
        }
        sb.append(body);

        return sb.toString();
    }
}
