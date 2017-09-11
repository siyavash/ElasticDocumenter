package ElasticDocument;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class RequestingThread extends Thread {
    private RestClient restClient;
    private static int REQUEST_THREAD_DOC_NUM = 6000;
    private ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue;


    public RequestingThread(String host, ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue) {
        restClient = RestClient.builder(new HttpHost(host, 9200, "http"), new HttpHost(host, 9201, "http")).build();

        this.pageInfoArrayBlockingQueue = pageInfoArrayBlockingQueue;
    }

    @Override
    public void run() {
        long t1;
        boolean iterationFinished = false;

        while (true) {
            ArrayList<PageInfo> pageInfos = new ArrayList<>();

            for (int i = 0; i < REQUEST_THREAD_DOC_NUM; i++) {
                try {
                    PageInfo pageInfo = pageInfoArrayBlockingQueue.take();
                    if (pageInfo.getUrl().equals("finished")) {
                        iterationFinished = true;
                        break;
                    }
                    pageInfos.add(pageInfo);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            t1 = System.currentTimeMillis();

            String requestBody = createRequestBody(pageInfos);
            System.out.println(requestBody);
            HttpEntity putEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
            try {
                restClient.performRequest("POST", "_bulk", Collections.emptyMap(), putEntity);
                Profiler.requestSent(REQUEST_THREAD_DOC_NUM);
                t1 = System.currentTimeMillis() - t1;
                Profiler.info("Request sent in " + t1 + " milli seconds");
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (iterationFinished) {
                break;
            }
        }

        try {
            restClient.close();
        } catch (IOException e) {
            Profiler.error("Failed to close REST client");
        }
    }


    private String createRequestBody(ArrayList<PageInfo> pageInfos) {
        StringBuilder finalRequest = new StringBuilder();
        Gson gson = new Gson();

        for (PageInfo pageInfo : pageInfos) {
            String id = createId(pageInfo.getUrl());

            finalRequest.append("{ \"index\" : { \"_index\" : \"gagoole\", \"_type\" : \"page\", \"_id\" : \"")
                    .append(id).append("\" } }");
            finalRequest.append("\n");

            String request = gson.toJson(pageInfo, PageInfo.class);

            finalRequest.append(request);
            finalRequest.append("\n");
        }

        return finalRequest.toString();
    }

    private String createId(String url) {
        String id = url.replaceAll("[^a-zA-Z0-9]", "");
        if (id.length() > 512) {
            StringBuilder newId = new StringBuilder(id.substring(0, 460));
            StringBuilder aux = new StringBuilder();
            int x = (id.length() - 512 + 40) / 40;
            for (int i = 0; i < 40; i++) {
                aux.append(id.charAt(460 + x * (i + 1)));
            }
            newId.append(aux);
            newId.append(id.substring(id.length() - 12, id.length()));
            id = newId.toString();
        }

        return id;
    }
}
