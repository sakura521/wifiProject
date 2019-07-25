package com.uestc.bigdata.flume.flumesource;

import com.uestc.bigdata.flume.constant.FlumeconfConstant;
import com.uestc.bigdata.flume.fields.MapFields;
import com.uestc.bigdata.flume.fileutil.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class FlumeSource extends AbstractSource implements PollableSource, Configurable {
    //1.定义常量
    private final Logger logger = Logger.getLogger(FlumeSource.class);

    private String getDir;
    private String[] dirs;
    private String successfile;
    private long sleepTime=20;
    private int filenum=5;

    private Collection<File> allFiles;
    private List<File> listFiles;
    private ArrayList<Event> eventList=new ArrayList<>();

    /**
     * 初始化flume配置
     * @param context
     */
    @Override
    public void configure(Context context) {
        logger.info("开始初始化flume配置...");
        initFlumeConfig(context);
        logger.info("初始化flume配置成功...");
    }


    /**
     * 开始监听处理数据
     * @return
     * @throws
     */
    @Override
    public Status process() {

        try {
            Thread.sleep(sleepTime*100);
        } catch (InterruptedException e) {
            logger.error(null, e);
        }

        Status status=null;

        try {
            allFiles=FileUtils.listFiles(new File(getDir),new String[] {"txt","csv"},true);
            if(allFiles.size()>=filenum){
                listFiles= ((List<File>) allFiles).subList(0,filenum);
            }else{
                listFiles=((List<File>) allFiles);
            }
            if(listFiles.size()>0){
                for(File file:listFiles){
                    String fileName = file.getName();

                    HashMap<String, Object> map = FileUtil.parseFile(file, successfile);
                    String absolute_filename = (String)map.get(MapFields.ABSOLUTE_FILENAME);
                    List<String> lines = (List<String>) map.get(MapFields.VALUE);

                    if(lines!=null && lines.size()>0 ){
                        for(String line:lines){
                            HashMap<String, String> mapString =new HashMap<>();
                            mapString.put(MapFields.FILENAME,fileName);
                            mapString.put(MapFields.ABSOLUTE_FILENAME,absolute_filename);

                            SimpleEvent event = new SimpleEvent();
                            byte[] bytes = line.getBytes();
                            // 消息体
                            event.setBody(bytes);
                            // 消息头
                            event.setHeaders(mapString);
                            eventList.add(event);
                        }

                        try {
                            if(eventList.size()>0){
                                ChannelProcessor channelProcessor=getChannelProcessor();
                                channelProcessor.processEventBatch(eventList);
                                logger.info("批量推送到 拦截器 数据大小为" + eventList.size());
                            }
                            eventList.clear();
                        } catch (Exception e) {
                            eventList.clear();
                            logger.error("发送数据到channel失败", e);
                        }finally {
                            eventList.clear();
                        }
                    }

                }
            }

            status=Status.READY;
            return status;
        } catch (Exception e) {
            status=Status.BACKOFF;
            logger.error("异常", e);
            return status;
        }

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


    /**
     * 初始化
     * @param context
     */
    private void initFlumeConfig(Context context) {
        try {
            getDir=context.getString(FlumeconfConstant.DIRS);
            dirs=getDir.split(",");
            successfile=context.getString(FlumeconfConstant.SUCCESSFILE);
            filenum=context.getInteger(FlumeconfConstant.FILENUM);
            sleepTime=context.getLong(FlumeconfConstant.SLEEPTIME);
            logger.info("dirStr============" + getDir);
            logger.info("dirs==============" + dirs);
            logger.info("successfile=======" + successfile);
            logger.info("filenum===========" + filenum);
            logger.info("sleeptime=========" + sleepTime);

        } catch (Exception e) {
            logger.error("初始化flume参数失败" + e);
        }
    }
}
