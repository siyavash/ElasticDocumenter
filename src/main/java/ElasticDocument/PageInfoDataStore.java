package ElasticDocument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class PageInfoDataStore
{
    private Connection hbaseConnection;
    private TableName tableName = TableName.valueOf("wb");
    private byte[] columnFamily = Bytes.toBytes("cf");
    private Logger logger = Logger.getLogger(Class.class.getName());

    public PageInfoDataStore(String zookeeperClientPort, String zookeeperQuorum) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConnection = ConnectionFactory.createConnection(configuration);
        logger.info("Connection to hbase established");
    }

    public Iterator<PageInfo> getRowIterator(long startTimeStamp, long stopTimeStamp, String lastCheckedURL) throws IOException
    {
        Table table = null;
        try
        {
            table = hbaseConnection.getTable(tableName);
            Scan scan;
            if (lastCheckedURL == null)
            {
                scan = new Scan();
            } else
            {
                scan = new Scan(Bytes.toBytes(lastCheckedURL));
            }

            if (startTimeStamp != -1 && stopTimeStamp != -1)
            {
                scan.setTimeRange(startTimeStamp, stopTimeStamp);
            }
            scan.setCaching(23);
            ResultScanner rowScanner = table.getScanner(scan);
            return new RowIterator(rowScanner);
        } finally
        {
            if (table != null)
            {
                table.close();
            }
        }
    }

    private PageInfo createPageInfo(Result result)
    {
        if (result == null)
        {
            return null;
        }

        PageInfo pageInfo = new PageInfo();

        pageInfo.setUrl(new String(result.getRow()));
        pageInfo.setBodyText(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("bodyText"))));
        pageInfo.setTitle(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("title"))));
        pageInfo.setAuthorMeta(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("authorMeta"))));
        pageInfo.setDescriptionMeta(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("descriptionMeta"))));
        pageInfo.setContentTypeMeta(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("contentTypeMeta"))));
        pageInfo.setKeyWordsMeta(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("keyWordsMeta"))));
        pageInfo.setNumOfInputLinks(toPageInfoInt(result.getValue(columnFamily, Bytes.toBytes("inputLinks"))));
//TODO: pageInfo.setInputAnchors(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("inputAnchors"))));
        pageInfo.setTitleMeta(toPageInfoString(result.getValue(columnFamily, Bytes.toBytes("titleMeta"))));

        return pageInfo;
    }

    private String toPageInfoString(byte[] bodyTexts)
    {
        if (bodyTexts == null)
        {
            return null;
        }

        return new String(bodyTexts);
    }

    private int toPageInfoInt(byte[] num){
        if (num == null)
        {
            return 0;
        }

        int value= 0;
        for(int i=0; i<num.length; i++)
            value = (value << 8) | num[i];
        return value;
    }

    private class RowIterator implements Iterator<PageInfo>
    {
        private ResultScanner rowScanner;

        private RowIterator(ResultScanner rowScanner)
        {
            this.rowScanner = rowScanner;
        }


        @Override
        public boolean hasNext()
        {
            try
            {
                return rowScanner.next() != null;
            } catch (IOException e)
            {
                return true;
            }
        }

        @Override
        public PageInfo next()
        {
            Result nextResult;

            try
            {
                long t1 = System.currentTimeMillis();

                nextResult = rowScanner.next();

                t1 = System.currentTimeMillis() - t1;
                if (t1 > 50)
                {
                    logger.warn("Getting the next object in the iterator took " + t1 + " milli seconds");
                }

            } catch (IOException e)
            {
                return null;
            }

            return createPageInfo(nextResult);
        }
    }
}
