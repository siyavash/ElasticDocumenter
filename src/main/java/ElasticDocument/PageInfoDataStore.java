package ElasticDocument;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
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

    public PageInfoDataStore() throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        hbaseConnection = ConnectionFactory.createConnection(configuration);
    }

    public boolean exists(String url) throws IOException
    {
        Table table = hbaseConnection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(url));
        Result result = table.get(get);
        table.close();
        return result.getRow() != null;
    }

    public void put(PageInfo pageInfo) throws IOException
    {
        String subLinks = turnSubLinksToString(pageInfo.getSubLinks());

        byte[] urlBytes = Bytes.toBytes(pageInfo.getUrl());
        Put put = new Put(urlBytes);

        Table table = hbaseConnection.getTable(tableName);

        addColumnToPut(put, Bytes.toBytes("authorMeta"), pageInfo.getAuthorMeta());
        addColumnToPut(put, Bytes.toBytes("descriptionMeta"), pageInfo.getDescriptionMeta());
        addColumnToPut(put, Bytes.toBytes("titleMeta"), pageInfo.getTitleMeta());
        addColumnToPut(put, Bytes.toBytes("contentTypeMeta"), pageInfo.getContentTypeMeta());
        addColumnToPut(put, Bytes.toBytes("keyWordsMeta"), pageInfo.getKeyWordsMeta());
        addColumnToPut(put, Bytes.toBytes("bodyText"), pageInfo.getBodyText());
        addColumnToPut(put, Bytes.toBytes("title"), pageInfo.getTitle());
        addColumnToPut(put, Bytes.toBytes("subLinks"), subLinks);

        table.put(put);
        table.close();
    }

    private void addColumnToPut(Put put, byte[] columnName, String value)
    {
        if (value == null)
        {
            return;
        }

        put.addColumn(columnFamily, columnName, Bytes.toBytes(value));
    }

    private String turnSubLinksToString(ArrayList<Pair<String, String>> subLinks)
    {
        if (subLinks == null)
        {
            return null;
        }

        StringBuilder stringBuilder = new StringBuilder();

        for (Pair<String, String> subLink : subLinks)
        {
            String linkName = "";
            if (subLink.getKey() != null)
            {
                linkName = subLink.getKey();
            }

            String anchorName = "";
            if (subLink.getValue() != null)
            {
                anchorName = subLink.getValue();
            }


            stringBuilder.append(linkName);
            stringBuilder.append(" , ");
            stringBuilder.append(anchorName);
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }

    public Iterator<PageInfo> getRowIterator(String lastCheckedURL) throws IOException
    {
        Table table = hbaseConnection.getTable(tableName);
        Scan scan;
        if (lastCheckedURL == null)
        {
            scan = new Scan();
        } else
        {
            scan = new Scan(Bytes.toBytes(lastCheckedURL));
        }
        scan.setCaching(25);
//        scan.setBatch(1000);
        ResultScanner rowScanner = table.getScanner(scan);
	
	table.close();
        // set caching
        return new RowIterator(rowScanner);
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

    private ArrayList<Pair<String, String>> extractSubLinks(Result result)
    {
        ArrayList<Pair<String, String>> subLinkPairs = new ArrayList<>();
        byte[] subLinksInBytes = result.getValue(columnFamily, Bytes.toBytes("subLinks"));

        if (subLinksInBytes == null)
        {
            return null;
        }
        String storedSubLinks = new String(subLinksInBytes);
        for (String subLink : storedSubLinks.split("\n"))
        {
            if (subLink.indexOf(',') == -1)
            {
                System.out.println(subLink);
                continue;
            }

            Pair<String, String> subLinkPair = extractSubLinkPair(subLink);
            subLinkPairs.add(subLinkPair);
        }

        return subLinkPairs;
    }

    private Pair<String, String> extractSubLinkPair(String subLink)
    {
//        System.out.println(subLink);
        String [] subLinkParts = subLink.split(" , ");
        String url = subLinkParts[0];

        if (subLinkParts.length < 2)
        {
            return new Pair<>(url, "");
        }

        String anchor = subLinkParts[1];

        return new Pair<>(url, anchor);
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
                nextResult = rowScanner.next();
            } catch (IOException e)
            {
                return null;
            }

            return createPageInfo(nextResult);
        }
    }
}