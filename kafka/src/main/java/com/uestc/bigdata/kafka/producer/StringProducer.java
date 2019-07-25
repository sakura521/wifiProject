package com.uestc.bigdata.kafka.producer;


import com.uestc.bigdata.kafka.Config.KafkaConfig;
import com.uestc.bigdata.common.threadPool.ThreadPoolManager;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class StringProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StringProducer.class);

    //1. 发送数据
    public static void producer(String topic,String record){
        Producer<String, String> producer = new Producer<>(KafkaConfig.getInstance().getProducerConfig());
        KeyedMessage<String,String> message = new KeyedMessage<>(topic, record);
        producer.send(message);
        LOG.info("发送数据"+record+"到kafka成功");
        producer.close();
    }

    //2. 批量发送
    public static void producerList(String topic, List<String> records){
        Producer<String, String> producer = new Producer<>(KafkaConfig.getInstance().getProducerConfig());

        List<KeyedMessage<String,String>> keyedMessageList=new ArrayList<>();
        for(String record:records){
            keyedMessageList.add(new KeyedMessage<>(topic,record));
        }
        producer.send(keyedMessageList);
        producer.close();
    }

    //3.多线程发送
    public  void producer(String topic, List<String> listMessage) throws Exception {

        List<List<String>>  splitsList=splitList(listMessage,5);
        int threadNum=splitsList.size();

        long t1 = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        ExecutorService executorService = ThreadPoolManager.getInstance().getExecutorServer();
        LOG.info("开启 " + threadNum + " 个线程来向  topic " + topic + " 生产数据 . ");
        for(int i=0;i<threadNum;i++){
            try {
                executorService.execute(new TaskProducer(topic, splitsList.get(i)));
                countDownLatch.countDown();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
        countDownLatch.await();
        long  t= System.currentTimeMillis() - t1;
        LOG.info(  " 一共耗时  ：" + t + "  毫秒 ... " );
        executorService.shutdown();

    }

    // 执行任务的线程
    class TaskProducer implements Runnable{

        private String topic;
        private List<String> listMeaasge;
        public TaskProducer(String topic,List<String> listMeaasge){
            this.topic=topic;
            this.listMeaasge=listMeaasge;
        }
        @Override
        public void run() {
            producerList(topic,listMeaasge);
        }
    }

    /**
     * 切分任务
     * @param listMessage
     * @return
     */
    private List<List<String>> splitList(List<String> listMessage,int splits) {
        if(listMessage==null || listMessage.size()<=0){
            return null;
        }

        int length = listMessage.size();
        int num=(length+splits-1)/splits;

        List<List<String>> listSplitMessage=new ArrayList<>(num);

        for(int i=0;i<num;i++){
            int start=i * splits;
            int end =(i+1) * splits >length? length:(i+1)* splits;
            listSplitMessage.add(listMessage.subList(start,end));

        }
        return listSplitMessage;

    }


}
