package ElasticDocument;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticDocumenter
{
    private PageInfoDataStore dataStore;
    private Logger logger = Logger.getLogger(Class.class.getName());
    private ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue = new ArrayBlockingQueue<>(10000);
//    private int requestCount;
    private int iterateCount;
    private static int REQUEST_THREAD_DOC_NUM = 15;

    public static void main(String[] args) throws IOException
    {
        PageInfoDataStore pageInfoDataStore = new PageInfoDataStore("2181", "master,slave");
        ElasticDocumenter elasticDocumenter = new ElasticDocumenter(pageInfoDataStore);

        elasticDocumenter.addDocuments();
    }

    public ElasticDocumenter(PageInfoDataStore dataStore)
    {
        this.dataStore = dataStore;
    }

    public void addDocuments() throws IOException
    {
        startIteratingThread();

        try
        {
            startSendingRequests(); //TODO close client
        } catch (InterruptedException e)
        {
            e.printStackTrace(); // TODO
        }
    }

    private void startSendingRequests() throws InterruptedException, IOException //TODO handle exceptions
    {
        RestClient restClient = RestClient.builder(new HttpHost("slave", 9200, "http"),
                new HttpHost("slave", 9201, "http")).build();

        long t1 = 0;

        while (true)
        {
            ArrayList<PageInfo> pageInfos = new ArrayList<>();

            for (int i = 0; i < REQUEST_THREAD_DOC_NUM; i++)
            {
                pageInfos.add(pageInfoArrayBlockingQueue.take());
            }

            t1 = System.currentTimeMillis();

            String requestBody = createRequestBody(pageInfos);
            HttpEntity putEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
            Response addingResponse = restClient.performRequest("POST", "_bulk", Collections.emptyMap(), putEntity);

            t1 = System.currentTimeMillis() - t1;

            logger.info("Request sent in " + t1 + " milli seconds");
            writeURLToFile(pageInfos.get(4).getUrl());
//            requestCount += 5;
//
//            if (requestCount % 100 == 0)
//            {
//                logger.info(requestCount + " request sent");
//            }
        }
    }

    private String createRequestBody(ArrayList<PageInfo> pageInfos)
    {
        StringBuilder finalRequest = new StringBuilder();
        Gson gson = new Gson();

        for (PageInfo pageInfo : pageInfos)
        {
            finalRequest.append("{ \"index\" : { \"_index\" : \"gagoole\", \"_type\" : \"page\"} }");
            finalRequest.append("\n");

            String request = gson.toJson(pageInfo, PageInfo.class);

            finalRequest.append(request);
            finalRequest.append("\n");
        }

        return finalRequest.toString();
    }

    private void startIteratingThread() throws IOException //TODO handle exce[ptions
    {
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                String lastCheckedURL = findLastURL();
                Iterator<PageInfo> pageInfoIterator = null;
                try
                {
                    pageInfoIterator = dataStore.getRowIterator(lastCheckedURL);
                } catch (IOException e)
                {
                    e.printStackTrace(); //TODO
                }
                PageInfo pageInfo;

                if (lastCheckedURL != null)
                {
                    pageInfoIterator.next();
                }

                while ((pageInfo = pageInfoIterator.next()) != null)
                {
                    try
                    {
                        pageInfoArrayBlockingQueue.put(pageInfo);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }

                    iterateCount++;


                    if (iterateCount % 100 == 0)
                    {
                        logger.info(iterateCount + " iteration done");
                    }
                }
            }
        }).start();
    }

    private String findLastURL()
    {
        BufferedReader br = null;
        FileReader fr = null;

        try
        {
            fr = new FileReader("lastUrlName.txt");
            br = new BufferedReader(fr);

            logger.info("url name file found");

            return br.readLine();

        } catch (IOException e)
        {
            if (e instanceof FileNotFoundException)
            {
                logger.warn("url name file not found");
                return null;
            }

            e.printStackTrace();
            return null;

        } finally
        {

            try
            {

                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();

            } catch (IOException ex)
            {
                ex.printStackTrace();
            }

        }
    }

    private void writeURLToFile(String url)
    {
        BufferedWriter bw = null;
        FileWriter fw = null;

        try
        {
            fw = new FileWriter("lastUrlName.txt");
            bw = new BufferedWriter(fw);
            bw.write(url);

//            logger.info("UrlName file updated");

        } catch (IOException e)
        {

            logger.warn("failed to update UrlName file");

        } finally
        {

            try
            {

                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();

            } catch (IOException ex)
            {

                ex.printStackTrace();

            }

        }
    }
}
