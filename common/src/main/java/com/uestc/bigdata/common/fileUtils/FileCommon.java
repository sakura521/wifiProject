package com.uestc.bigdata.common.fileUtils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class FileCommon {

    /**
     * 获得路径下的绝对路径
     * @param path
     * @return
     * @throws IOException
     */
    public static String getAbstractPath(String path) throws IOException {
        URL url = FileCommon.class.getClassLoader().getResource(path);
        File file = new File(url.getFile());
        String content = FileUtils.readFileToString(file);
        return content;
    }
}
