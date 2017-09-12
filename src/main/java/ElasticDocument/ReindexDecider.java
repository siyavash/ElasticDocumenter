package ElasticDocument;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class ReindexDecider extends Thread {
    private ArrayBlockingQueue<PageInfo> pageInfoFromHbase;
    private ArrayBlockingQueue<PageInfo> pageInfoToElastic;

    public ReindexDecider(ArrayBlockingQueue<PageInfo> pageInfoFromHbase, ArrayBlockingQueue<PageInfo> pageInfoToElastic) {
        this.pageInfoFromHbase = pageInfoFromHbase;
        this.pageInfoToElastic = pageInfoToElastic;
    }

    @Override
    public void run() {
        while (true) {
            PageInfo newPageInfo = new PageInfo();
            try {
                newPageInfo = pageInfoFromHbase.take();
            } catch (InterruptedException e) {
                continue;
            }
            String url = newPageInfo.getUrl();
            Response response = getFromElastic(url);
            PageInfo existPageInfo = convertToPageInfo(response);
            boolean needReindex = checkIfNeedReindex(newPageInfo, existPageInfo);
            if (needReindex){
                try {
                    pageInfoToElastic.put(newPageInfo);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private boolean checkIfNeedReindex(PageInfo newPageInfo, PageInfo existPageInfo) {
        int newPageInfoInputLink, existPageInfoInputLink;
        newPageInfoInputLink = newPageInfo.getNumOfInputLinks();
        existPageInfoInputLink = existPageInfo.getNumOfInputLinks();
        return ((newPageInfoInputLink - existPageInfoInputLink) / existPageInfoInputLink) <= 0.1;
    }

    private static Response getFromElastic(String url) {
        RestClient restClient = RestClient.builder(new HttpHost("master", 9200, "http"),
                new HttpHost("master", 9201, "http")).build();
        HttpEntity getEntity = new NStringEntity("", ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = restClient.performRequest("GET", "/gagoole/page/" + getUrlID(url) + "/_source", Collections.emptyMap(), getEntity);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    private static PageInfo convertToPageInfo(Response response){
        Gson gson = new Gson();
        PageInfo pageInfo = new PageInfo();
        try {
            String responseString = EntityUtils.toString(response.getEntity());
            System.out.println(responseString);
            pageInfo = gson.fromJson(responseString, PageInfo.class);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return pageInfo;
    }

    private static String getUrlID(String url)
    {
        String id = url.replaceAll("[^a-zA-Z0-9]", "");
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
