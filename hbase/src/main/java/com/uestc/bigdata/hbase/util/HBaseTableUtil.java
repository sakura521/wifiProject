package com.uestc.bigdata.hbase.util;


import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 建表的工具类:静态方法  可以直接类名.方法调用
 * 1.获得HBaseConf
 * Configuration=HBaseConfiguration.create();
 * 2.获得HBaseConnection
 * Connection connection = ConnectionFactory.createConnection(conf);
 * 3.获得表管理者
 * HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
 * 4.创建一个描述器
 *       HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
 *        创建列族
 *         for(String cf:columnFamilly){
 *              htd.addFamily(new HColumnDescriptor(cf));
 *        }
 *        创建表
 *        admin.createTable(htd);
 *
 *  1：判断表是否存在
 *  2：关闭表
 *  3：清空表
 *  4：删除表
 *  5：获取HBase表
 *  6：列出所有的表
 *
 *  7：创建表
 *  8：创建命名空间
 *  9：创建表描述器
 *
 *
 */
public class HBaseTableUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);
    private static final String COPROCESSORCLASSNAME =  "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
    private static  HBaseConf hbaseConf=HBaseConf.getInstance();



    /**
     * 1.判断表是否存在
     * hbaseConf.getHConnection().getAdmin().tableExists(TableName.valueOf(tableName))
     * @param tableName
     * @return
     */
    public static boolean isExists(String tableName){
        boolean exists=false;
        try {
             exists = hbaseConf.getHConnection().getAdmin().tableExists(TableName.valueOf(tableName));
        } catch (MasterNotRunningException e) {
            LOG.error("HBase  master  未运行 。 ", e);
        }catch (ZooKeeperConnectionException e){
            LOG.error("zooKeeper 连接异常。 ", e);
        }catch (IOException e){
            LOG.error("", e);
        }
        return exists;
    }



    /**
     * 2.获取HBase中的表
     * hbaseConf.getHConnection().getTable(TableName.valueOf(tableName))
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName){
        Table table=null;
        try {
            table = hbaseConf.getHConnection().getTable(TableName.valueOf(tableName));
        }catch (MasterNotRunningException e) {
            LOG.error("HBase  master  未运行 。 ", e);
        }catch (ZooKeeperConnectionException e){
            LOG.error("zooKeeper 连接异常。 ", e);
        }catch (IOException e){
            LOG.error("", e);
        }
        return table;
    }


    /**
     * 关闭表
     * @param table
     */
    public static void close(Table table){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }


    /**
     * 4.清空表
     * admin.tableExists(name)
     * admin.isTableAvailable(name)
     * admin.disableTable(name);
     *  admin.truncateTable(name,true);
     * @param tableName
     * @return
     */
    public static boolean truncateTable(String tableName){
        boolean status=false;
        TableName name = TableName.valueOf(tableName);
        try {
            Admin admin = hbaseConf.getHConnection().getAdmin();
            if(admin.tableExists(name)){
                if(admin.isTableAvailable(name)){
                    admin.disableTable(name);
                }
                admin.truncateTable(name,true);
            }else{
                LOG.warn(" HBase中不存在 表 " + tableName);
            }
            admin.close();
            status=true;
        }catch (MasterNotRunningException e) {
            LOG.error("HBase  master  未运行 。 ", e);
        }catch (ZooKeeperConnectionException e){
            LOG.error("zooKeeper 连接异常。 ", e);
        }catch (IOException e){
            LOG.error("", e);
        }
        return status;
    }

    /**
     * 5.删除表
     * admin.tableExists(name)
     * admin.isTableAvailable(name)
     * admin.disableTable(name);
     * admin.deleteTable(name);
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName){
        boolean status=false;
        TableName name = TableName.valueOf(tableName);

        try {
            Admin admin = hbaseConf.getHConnection().getAdmin();
            if(admin.tableExists(name)){
                if(admin.isTableAvailable(name)){
                    admin.disableTable(name);
                }
                admin.deleteTable(name);
            }else{
                LOG.warn(" HBase中不存在 表 " + tableName);
            }

            admin.close();
            status=true;
        }catch (MasterNotRunningException e) {
            LOG.error("HBase  master  未运行 。 ", e);
        }catch (ZooKeeperConnectionException e){
            LOG.error("zooKeeper 连接异常。 ", e);
        }catch (IOException e){
            LOG.error("", e);
        }

        return status;
    }

    /**
     * 6.获取HBase所有的表名
     * @return
     */
    public static List<String> listTables(){
        List<String> lists=new ArrayList<>();
        Admin admin=null;
        try {
            admin= hbaseConf.getHConnection().getAdmin();

            TableName[] names = admin.listTableNames();

            for(TableName name:names){
                lists.add(name.getNameAsString());
            }
        } catch (IOException e) {
            LOG.error("获取HBase表失败。", e);
        }finally {
            if(admin!=null){
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }
        return lists;
    }


    /**
     * 7.创建表
     * @param tableName  表名
     * @param cf     列族
     * @param inMemory    是否存在内存
     * @param ttl       过期时间策略
     * @param maxVersion   最大版本号
     * @param splits    分区
     * @return
     */
    public static boolean createTable(String tableName,
                                      String cf,
                                      boolean inMemory,
                                      int ttl,
                                      int maxVersion,
                                      byte[][] splits){
        //返回表描述器
        HTableDescriptor htd=  createHbaseTableDescriptor(tableName,cf,inMemory,ttl,maxVersion,COPROCESSORCLASSNAME);
        //通过HTableDescriptor 和 splits 分区策略来定义表
        return createTable( htd,splits);
    }

    /**
     * 11.通过HTableDescriptor 和 splits 分区策略来定义表
     * @param htd
     * @param splits
     * @return
     */
    private static boolean createTable(HTableDescriptor htd, byte[][] splits) {

        Admin admin=null;
        try {
            TableName tableName = htd.getTableName();
            admin=hbaseConf.getHConnection().getAdmin();
            boolean exists=admin.tableExists(tableName);
            if(exists){
                LOG.error("表"+tableName.getNameAsString() + "已经存在");
            }else{
                admin.createTable(htd,splits);
            }
        } catch (IOException e) {
            LOG.error("创建HBase表失败。", e);
            return false;
        } finally {
            if(admin!=null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    /**
     * 8.创建表描述器
     * @param tableName
     * @param cf
     * @param inMemory
     * @param ttl
     * @param maxVersion
     * @param coprocessorclassname
     * @return
     */
    private static HTableDescriptor createHbaseTableDescriptor(String tableName,
                                                               String cf,
                                                               boolean inMemory,
                                                               int ttl,
                                                               int maxVersion,
                                                               String... coprocessorclassname) {
        return createHbaseTableDescriptor(tableName,cf,inMemory,ttl,maxVersion,true,coprocessorclassname);
    }

    /**
     * 9.开启压缩方式创建表描述器
     * @param tableName
     * @param cf
     * @param inMemory
     * @param ttl
     * @param maxVersion
     * @param useSnappy
     * @param coprocessorclassnames
     * @return
     */
    private static HTableDescriptor createHbaseTableDescriptor(String tableName, String cf, boolean inMemory, int ttl, int maxVersion, boolean useSnappy, String... coprocessorclassnames) {
        // 根据表名，创建命名空间
        String[] split = tableName.split(":");
        if(split.length==2){
            createNameSpace(split[0]);
        }

        // 创建表描述器
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        // 添加协处理器
        for(String coprocessorclassname:coprocessorclassnames){
            try {
                htd.addCoprocessor(coprocessorclassname);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //创建列描述器 添加性能
        HColumnDescriptor hcd = new HColumnDescriptor(cf);

        // 定义最大版本号
        if(maxVersion>0){
            hcd.setMaxVersions(maxVersion);
        }


        /**
         * 设置布隆过滤器  优化
         * 默认是NONE 是否使用布隆过虑及使用何种方式
         * 布隆过滤可以每列族单独启用
         * Default = ROW 对行进行布隆过滤。
         * 对 ROW，行键的哈希在每次插入行时将被添加到布隆。
         * 对 ROWCOL，行键 + 列族 + 列族修饰的哈希将在每次插入行时添加到布隆
         * 使用方法: create ‘table’,{BLOOMFILTER =>’ROW’}
         * 启用布隆过滤可以节省读磁盘过程，可以有助于降低读取延迟
         */
        hcd.setBloomFilterType(BloomType.ROWCOL);


        /**
         * hbase在LRU缓存基础之上采用了分层设计，整个blockcache分成了三个部分，分别是single、multi和inMemory。三者区别如下：
         * single：如果一个block第一次被访问，放在该优先队列中；
         * multi：如果一个block被多次访问，则从single队列转移到multi队列
         * inMemory：优先级最高，常驻cache，因此一般只有hbase系统的元数据，如meta表之类的才会放到inMemory队列中。普通的hbase列族也可以指定IN_MEMORY属性，方法如下：
         * create 'table', {NAME => 'f', IN_MEMORY => true}
         * 修改上表的inmemory属性，方法如下：
         * alter 'table',{NAME=>'f',IN_MEMORY=>true}
         */
        hcd.setInMemory(inMemory);
        hcd.setScope(1);


        /**
         *数据量大，边压边写也会提升性能的，毕竟IO是大数据的最严重的瓶颈，
         * 		哪怕使用了SSD也是一样。众多的压缩方式中，推荐使用SNAPPY。从压缩率和压缩速度来看，
         * 		性价比最高。
         */
        if(useSnappy){
            hcd.setCompressionType(Compression.Algorithm.SNAPPY);
        }

        /**
         * 默认为NONE
         *         如果数据存储时设置了编码， 在缓存到内存中的时候是不会解码的，这样和不编码的情况相比，相同的数据块，编码后占用的内存更小， 即提高了内存的使用率
         *         如果设置了编码，用户必须在取数据的时候进行解码， 因此在内存充足的情况下会降低读写性能。
         *         在任何情况下开启PREFIX_TREE编码都是安全的
         *         不要同时开启PREFIX_TREE和SNAPPY
         *         通常情况下 SNAPPY并不能比 PREFIX_TREE取得更好的优化效果
         */
       // hcd.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);

        /**
         * 默认为64k     65536
         *         随着blocksize的增大， 系统随机读的吞吐量不断的降低，延迟也不断的增大，
         *         64k大小比16k大小的吞吐量大约下降13%，延迟增大13%
         *         128k大小比64k大小的吞吐量大约下降22%，延迟增大27%
         *         对于随机读取为主的业务，可以考虑调低blocksize的大小
         *
         *         随着blocksize的增大， scan的吞吐量不断的增大，延迟也不断降低，
         *         64k大小比16k大小的吞吐量大约增加33%，延迟降低24%
         *         128k大小比64k大小的吞吐量大约增加7%，延迟降低7%
         *         对于scan为主的业务，可以考虑调大blocksize的大小
         *
         *         如果业务请求以Get为主，则可以适当的减小blocksize的大小
         *         如果业务是以scan请求为主，则可以适当的增大blocksize的大小
         *         系统默认为64k, 是一个scan和get之间取的平衡值
         *         hcd.setBlocksize(s)
         */

        // 有这个存在的时候建表失败
       // hcd.setBlocksize(64);

        /**
         * 设置表中数据的存储生命期，过期数据将自动被删除，
         *         例如如果只需要存储最近两天的数据，s
         *         那么可以设置setTimeToLive(2 * 24 * 60 * 60)
         */
        if(ttl<0){
            ttl = HConstants.FOREVER;
        }
        hcd.setTimeToLive(ttl);


        // 添加到表描述器中
        htd.addFamily(hcd);

        return htd;
    }

    /**
     * 10.创建命名空间
     * 首先获得所有的命名空间，存在就返回，不存在就创建
     * NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
     *  admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
     * @param nameSpace
     */
    private static boolean createNameSpace(String nameSpace) {
        Admin admin=null;
        try {
            boolean exists=false;
            admin=hbaseConf.getHConnection().getAdmin();
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for(NamespaceDescriptor namespaceDescriptor:namespaceDescriptors){
                if(namespaceDescriptor.equals(nameSpace)){
                    exists=true;
                }
            }
            if(!exists){
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
            }
        } catch (IOException e) {
            LOG.error("创建HBase命名空间失败。", e);
            return false;
        } finally {
            if(admin!=null){
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }
        return true;
    }

    public static void main(String[] args) throws Exception{
        HBaseTableUtil.truncateTable("test:imei");
        HBaseTableUtil.truncateTable("test:imsi");
        HBaseTableUtil.truncateTable("test:phone");
        HBaseTableUtil.truncateTable("test:phone_mac");
        HBaseTableUtil.truncateTable("test:relation");
        HBaseTableUtil.truncateTable("test:send_mail");
        HBaseTableUtil.truncateTable("test:username");
        HBaseTableUtil.truncateTable("test:chl_test8");

      /* String  hbase_table="test:chl_test2";
       *//* HBaseTableUtil.createTable(hbase_table,
                "cf",
                true,
                -1,
                100,
                SplitskeyRegion.getSplitsRowkerDist());*/

        HBaseTableUtil.deleteTable("cache:phone");
        HBaseTableUtil.deleteTable("huang");

    }
}
