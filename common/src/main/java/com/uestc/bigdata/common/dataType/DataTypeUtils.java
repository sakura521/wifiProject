package com.uestc.bigdata.common.dataType;

import com.uestc.bigdata.common.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *  处理配置文件dataType  输出
 *  hashmap(qq,arraylist(字段值))
 */
public class DataTypeUtils {
    private static Logger LOG= LoggerFactory.getLogger(DataTypeUtils.class);

    private static final String DATAPATH="common/datatype.properties";

    public static HashMap<String, ArrayList<String>> dataType=null;

    // 静态代码块加载
      static{
          Properties properties = ConfigUtil.getInstance().getProperties(DATAPATH);
          dataType=new HashMap<>();
          Set<Object> keys = properties.keySet();
          keys.forEach(key->{
              String[] fields = properties.getProperty(key.toString()).split(",");
              dataType.put(key.toString(),new ArrayList<>(Arrays.asList(fields)));
          });
      }

    public static void main(String[] args) {
        HashMap<String, ArrayList<String>> dataType = DataTypeUtils.dataType;
        System.out.println(dataType.toString());
    }
}
