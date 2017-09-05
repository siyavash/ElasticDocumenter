package ElasticDocument;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
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
    private static int REQUEST_THREAD_DOC_NUM = 500;
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
        boolean iterationFinished = false;

        while (true)
        {
            ArrayList<PageInfo> pageInfos = new ArrayList<>();

            for (int i = 0; i < REQUEST_THREAD_DOC_NUM; i++)
            {
                try
                {
                    PageInfo pageInfo = pageInfoArrayBlockingQueue.take();
                    if (pageInfo == null)
                    {
                        iterationFinished = true;
                        break;
                    }
                    pageInfos.add(pageInfo);
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
                Profiler.requestSent(REQUEST_THREAD_DOC_NUM);
            } catch (IOException e)
            {
                e.printStackTrace(); //TODO
            }

            t1 = System.currentTimeMillis() - t1;

            Profiler.info("Request sent in " + t1 + " milli seconds");
            writeURLToFile(pageInfos.get(pageInfos.size() - 1).getUrl());

            if (iterationFinished)
            {
                break;
            }
        }

        try
        {
            restClient.close();
        } catch (IOException e)
        {
            Profiler.error("Failed to close REST client");
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

            Profiler.error("failed to update UrlName file");

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
            String id = createId(pageInfo.getUrl());

            finalRequest.append("{ \"index\" : { \"_index\" : \"gagoole\", \"_type\" : \"page\", \"_id\" : \"").append(id).append("\" } }");
            finalRequest.append("\n");

            String request = gson.toJson(pageInfo, PageInfo.class);

            finalRequest.append(request);
            finalRequest.append("\n");
        }

        return finalRequest.toString();
    }

    private String createId(String url)
    {
        String id = url.replaceAll("[^a-zA-Z]", "");
        if (id.length() > 512)
        {
            StringBuilder newId = new StringBuilder(id.substring(0, 460));
            StringBuilder aux = new StringBuilder();
            int x = (id.length() - 512 + 40) / 40;
            for (int i = 0; i < 40; i++)
            {
                aux.append(id.charAt(460 + x * (i + 1)));
            }
            newId.append(aux);
            newId.append(id.substring(id.length() - 12, id.length()));
            id = newId.toString();
        }

        return id;
    }
}
