package ElasticDocument;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

public class PageInfoDataStore {
    private Connection hbaseConnection;
    private TableName tableName = TableName.valueOf("wb");
    private byte[] columnFamily = Bytes.toBytes("cf");
    private Logger logger = Logger.getLogger(Class.class.getName());

    public PageInfoDataStore(String zookeeperClientPort, String zookeeperQuorum) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConnection = ConnectionFactory.createConnection(configuration);
        logger.info("Connection to hbase established");
    }

    public Iterator<PageInfo> getRowIterator(long startTimeStamp, long stopTimeStamp)
            throws IOException {
        try (Table table = hbaseConnection.getTable(tableName)) {
            Scan scan = new Scan();
            if (startTimeStamp != -1 && stopTimeStamp != -1) {
                scan.setTimeRange(startTimeStamp, stopTimeStamp);
            }
            scan.setCaching(23);
            ResultScanner rowScanner = table.getScanner(scan);
            return new RowIterator(rowScanner);
        }
    }

    private PageInfo createPageInfo(Result result) {
        if (result == null) {
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
        pageInfo.setNumOfInputLinks(toPageInfoInt(result.getValue(columnFamily, Bytes.toBytes("numOfInputLinks"))));
        pageInfo.setPageRank(toPageInfoDouble(result.getValue(columnFamily, Bytes.toBytes("pr"))));
        pageInfo.setInputAnchors(toPageInfoInputAnchors(result.getValue(columnFamily, Bytes.toBytes("anchors"))));
        return pageInfo;
    }

    private double toPageInfoDouble(byte[] num) {
        if (num == null)
        {
            return 0;
        }

        return ByteBuffer.wrap(num).getDouble();
    }

    private ArrayList<Pair<String, Integer>> toPageInfoInputAnchors(byte[] anchors) {
        if (anchors == null) {
            return null;
        }
        ArrayList<Pair<String, Integer>> anchorsList = new ArrayList<>();
        String[] splitedAnchors = new String(anchors).split("\n");
        for (int i = 0; i < splitedAnchors.length; i+=2) {
            try {
                anchorsList.add(new Pair<>(splitedAnchors[i], Integer.parseInt(splitedAnchors[i])));
            }
            catch (NumberFormatException e){
                e.printStackTrace();
            }
        }
        return anchorsList;
    }

    private String toPageInfoString(byte[] bodyTexts) {
        if (bodyTexts == null) {
            return null;
        }

        return new String(bodyTexts);
    }

    private int toPageInfoInt(byte[] num) {
        if (num == null) {
            return 0;
        }

        int value = 0;
        for (int i = 0; i < num.length; i++)
            value = (value << 8) | num[i];
        return value;
    }

    private class RowIterator implements Iterator<PageInfo> {
        private Iterator<Result> rowIterator;

        private RowIterator(ResultScanner rowScanner) {
            this.rowIterator = rowScanner.iterator();
        }


        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        @Override
        public PageInfo next() {
            long t1 = System.currentTimeMillis();

            Result nextResult = rowIterator.next();

            t1 = System.currentTimeMillis() - t1;
            if (t1 > 50) {
                logger.warn("Getting the next object in the iterator took " + t1 + " milli seconds");
            }

            return createPageInfo(nextResult);
        }
    }
}
