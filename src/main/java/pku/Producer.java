package pku;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wanghui on 2017/12/14.
 */
public class Producer {
    private List<String> topics = new ArrayList<>();

    public ByteMessage createBytesMessageToTopic(String topic, byte[] body)throws Exception{
        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC, topic);

//        Store.store.init(topic);
        StreamStore.store.init(topic);
        topics.add(topic);

        return msg;
    }

    public void send(ByteMessage defaultMessage)throws Exception{

        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
//        Store.store.write(defaultMessage, topic);
        StreamStore.store.write(defaultMessage, topic);
    }

    public void flush()throws Exception{
        for (String topic: topics) {
//            Store.store.flush(topic);
            StreamStore.store.flush(topic);
        }

    }
}
