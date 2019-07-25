package com.uestc.bigdata.flume.sink;

import com.google.common.base.Throwables;
import com.uestc.bigdata.kafka.producer.StringProducer;
import kafka.producer.KeyedMessage;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class FlumeSink extends AbstractSink implements Configurable {
    private final Logger logger = Logger.getLogger(FlumeSink.class);
    private String[] kafkatopics;
    private List<KeyedMessage<String,String>> messageList=null;

    @Override
    public Status process() throws EventDeliveryException {

        logger.info("sink开始执行");
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        try {
            Event event = channel.take();
            if(event==null){
                transaction.rollback();
                return Status.BACKOFF;
            }

            String record = new String(event.getBody());

            try {
                StringProducer.producer(kafkatopics[0],record);
            } catch (Exception e) {
                logger.error("推送数据到kafka失败" , e);
                throw Throwables.propagate(e);
            }

            transaction.commit();
            return Status.READY;
        } catch (ChannelException e) {
            logger.error(e);
            transaction.rollback();
            return Status.BACKOFF;
        } finally {
            if(transaction!=null){
                transaction.close();
            }
        }

    }

    @Override
    public void configure(Context context) {
        kafkatopics= context.getString("kafkatopics").split(",");
        messageList=new ArrayList<>();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
