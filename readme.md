
# Introduction
This project implemented Message Queue and persist messages by topic in disk storage.

Environment:   
	4-core machine   
	4 Gigabytes memory   
	JVM memory 2.6G -Xms2560M -Xmx2560M  
	disk speed: 40MB/s  
	
	
# Architecture

![Architecture](https://raw.githubusercontent.com/wanghuiss/javamq/master/src/main/images/message_queue_architecture.png)

# Core Classes
**Producer**

`createBytesMessageToTopic(topic, body)` create a message and specify the topic.

`send(message)` send a message

`flush()` being called when sending message is done

**Consumer**

`attachQueue(queue, topics)` attach Topics to Queue

`poll()` pull message

**KeyValue**

**ByteMessage**

**MessageHeader**

**Store:** compress message header using bit representation