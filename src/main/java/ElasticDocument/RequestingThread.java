package ElasticDocument;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class RequestingThread extends Thread
{
    private RestClient restClient; //TODO implement it in a way to be able to close it
    private static int REQUEST_THREAD_DOC_NUM = 200;
    private Logger logger = Logger.getLogger(Class.class.getName());
    private ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue;


    public RequestingThread(String host, ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue)
    {
        restClient = RestClient.builder(new HttpHost(host, 9200, "http")).build();

        this.pageInfoArrayBlockingQueue = pageInfoArrayBlockingQueue;
    }

    @Override
    public void run()
    {
        long t1;

        while (true)
        {
            ArrayList<PageInfo> pageInfos = new ArrayList<>();

            for (int i = 0; i < REQUEST_THREAD_DOC_NUM; i++)
            {
                try
                {
                    pageInfos.add(pageInfoArrayBlockingQueue.take());
                } catch (InterruptedException e)
                {
                    e.printStackTrace(); //TODO
                }
            }

            t1 = System.currentTimeMillis();

            String requestBody = createRequestBody(pageInfos);
            HttpEntity putEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
            try
            {
                Response addingResponse = restClient.performRequest("POST", "_bulk", Collections.emptyMap(), putEntity);
            } catch (IOException e)
            {
                e.printStackTrace(); //TODO
            }

            t1 = System.currentTimeMillis() - t1;

            logger.info("Request sent in " + t1 + " milli seconds");
            writeURLToFile(pageInfos.get(4).getUrl());
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
}
