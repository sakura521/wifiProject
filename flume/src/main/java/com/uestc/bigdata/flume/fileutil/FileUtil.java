package com.uestc.bigdata.flume.fileutil;


import com.uestc.bigdata.flume.fields.MapFields;
import com.uestc.bigdata.common.time.TimeTransformUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import static java.io.File.separator;

/**
 * 原始文件上传到新的文件目录中
 * 1.按照日期创建新的文件目录；获取原始文件的父目录
 * 2.判断新的文件是否存在，存在就删除
 * 3.不存在，读取原始文件内容，进行封装map(文件目录，文件内容)
 * 4.移动文件
 */
public class FileUtil {
    private static Logger LOG = LoggerFactory.getLogger(FileUtil.class);


    public static HashMap<String,Object> parseFile(File file, String path){
        HashMap<String,Object> map=new HashMap<>();
        List<String> lines;
        String fileNew=path+ TimeTransformUtils.Date2yyyy_MM_dd()+getDir(file);
        try {
            if (new File(fileNew + file.getName()).exists()) {
                try {
                    // 防止重复消费文件数据
                    LOG.info("文件已经存在" + file.getAbsolutePath() + "开始删除已经存在文件");
                    file.delete();
                    LOG.info("删除同名已经存在文件" + file.getAbsolutePath() + "成功");
                } catch (Exception e) {
                    LOG.error("删除同名已经存在文件" + file.getAbsolutePath() + "失败");
                }
            } else {
                lines = FileUtils.readLines(file);
                map.put(MapFields.ABSOLUTE_FILENAME, fileNew+file.getName());
                map.put(MapFields.VALUE,lines);
                FileUtils.moveFileToDirectory(file,new File(fileNew),true);
                LOG.info("移动文件到" + fileNew + "成功");
            }
        }catch (Exception e) {
            LOG.error("移动文件" + file.getAbsolutePath() + "失败");
            }
        return map;
    }

    /**
     * 获取二级父目录
     * @param file
     * @return
     */
    private static String getDir(File file) {
        String dir = file.getParent();
        String str="";
        StringTokenizer tokenizer = new StringTokenizer(dir, separator);
        ArrayList<String> list=new ArrayList<>();
        while(tokenizer.hasMoreTokens()){
            list.add((String)tokenizer.nextElement());
        }
        for(int i=2;i<list.size();i++){
            str=str+separator+list.get(i);
        }
        return str+"/";
    }

    public static void main(String[] args) {
        String dir = getDir(new File("F:\\data\\in\\geneFile.txt"));
        System.out.println(dir);
    }

}
