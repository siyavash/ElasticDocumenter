package ElasticDocument;

import com.google.gson.Gson;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class ReindexDecider {
    private ArrayBlockingQueue<PageInfo> pageInfoFromHbase;
    private ArrayBlockingQueue<PageInfo> pageInfoToElastic;

    public static void main(String[] args) {
        ReindexDecider r = new ReindexDecider();
        System.out.println(convertToPageInfo(getFromElastic("http://ubucon.org")).getBodyText());
        System.out.println(convertToPageInfo(getFromElastic("http://ubucon.org")).getTitle());
    }

    public ReindexDecider(/*ArrayBlockingQueue<PageInfo> pageInfoFromHbase, ArrayBlockingQueue<PageInfo> pageInfoToElastic*/) {
        //this.pageInfoFromHbase = pageInfoFromHbase;
        //this.pageInfoToElastic = pageInfoToElastic;
    }


    private static Response getFromElastic(String url) {
        RestClient restClient = RestClient.builder(new HttpHost("master", 9200, "http"),
                new HttpHost("master", 9201, "http")).build();
        HttpEntity getEntity = null;
        getEntity = new NStringEntity("", ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = restClient.performRequest("GET", "/gagoole/page/" + createId(url) + "/_source", Collections.emptyMap(), getEntity);
        } catch (IOException e) {

        }
        return response;
    }

    private static PageInfo convertToPageInfo(Response response){
        Gson gson = new Gson();
        PageInfo pageInfo = null;
        try {
            pageInfo = gson.fromJson(response.getEntity().getContent().toString(), PageInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pageInfo;
    }

    private static String createId(String url)
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
