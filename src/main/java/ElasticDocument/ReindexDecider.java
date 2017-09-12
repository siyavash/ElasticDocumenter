package ElasticDocument;

import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class ReindexDecider extends Thread {
    private ArrayBlockingQueue<PageInfo> pageInfoFromHbase;
    private ArrayBlockingQueue<PageInfo> pageInfoToElastic;

    public ReindexDecider(ArrayBlockingQueue<PageInfo> pageInfoFromHbase, ArrayBlockingQueue<PageInfo> pageInfoToElastic) {
        this.pageInfoFromHbase = pageInfoFromHbase;
        this.pageInfoToElastic = pageInfoToElastic;
    }

    public static void main(String[] args) {

        PageInfo pageInfo = new PageInfo();
        ArrayList<Pair<String, Integer>> arrayList = new ArrayList<>();
        arrayList.add(new Pair<>("LOL", 1));
        arrayList.add(new Pair<>("FML", 2));
        pageInfo.setInputAnchors(arrayList);
        System.out.println(new Gson().toJson(pageInfo, PageInfo.class));
//        System.out.println(convertToPageInfo(getFromElastic("http://ubucon.org")));
    }

    @Override
    public void run() {
        while (true) {
            PageInfo newPageInfo;
            try {
                newPageInfo = pageInfoFromHbase.take();
            } catch (InterruptedException e) {
                continue;
            }
            String url = newPageInfo.getUrl();
            Response response = getFromElastic(url);
            PageInfo existPageInfo = convertToPageInfo(response);
            if (existPageInfo == null){
                continue;
            }
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
        int newPageInputLink = newPageInfo.getNumOfInputLinks();
        int existPageInputLink = existPageInfo.getNumOfInputLinks();
        double newPageRank = newPageInfo.getPageRank();
        double existPageRank = existPageInfo.getPageRank();
        return ((((double)newPageInputLink - (double)existPageInputLink) / (double)existPageInputLink) >= 0.1) || (newPageRank - existPageRank >= 1.0);
    }

    private static Response getFromElastic(String url) {
        RestClient restClient = RestClient.builder(new HttpHost("master", 9200, "http"),
                new HttpHost("master", 9201, "http")).build();
        HttpEntity getEntity = new NStringEntity("", ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = restClient.performRequest("GET", "/gagoolev2/page/" + getUrlID(url) + "/_source", Collections.emptyMap(), getEntity);
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
//            System.out.println(responseString);
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
