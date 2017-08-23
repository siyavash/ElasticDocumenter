package ElasticSearch;

import ElasticDocument.PageInfoDataStore;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;

public class Searcher
{
    public static void main(String[] args) throws IOException
    {
        Searcher elasticIndexer = new Searcher();
        elasticIndexer.startSearching();
    }

    public void startSearching() throws IOException
    {
        Scanner in = new Scanner(System.in);

        RestClient restClient = RestClient.builder(new HttpHost("master", 9200, "http"),
                new HttpHost("master", 9201, "http")).build();

        while (true)
        {
            String input = in.nextLine();
            if (input.equals("pls exit"))
            {
                restClient.close();
                return;
            }
            String searchQuery = createQuery(input);

            HttpEntity getEntity = new NStringEntity(searchQuery, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("POST", "/gagoole/_search?pretty=true"/* + pageInfo.getUrl()*/, Collections.emptyMap(), getEntity);
            String responseString = EntityUtils.toString(response.getEntity());
            showResults(responseString);
        }

    }

    private void showResults(String responseString)
    {

        responseString = "{ \"obj\":" + responseString + "}";
        JSONObject jsonObject = new JSONObject(responseString).getJSONObject("obj");
        JSONArray hits = jsonObject.getJSONObject("hits").getJSONArray("hits");

        if (hits.length() == 0)
        {
            System.out.println("No results found!");
            return;
        }

        System.out.println("Results: ");

        for (int i = 0; i < hits.length(); i++)
        {
            JSONObject json = hits.getJSONObject(i).getJSONObject("_source");
            if (json.has("url"))
            {
                System.out.println(json.getString("url"));
            }
        }
    }

    private String createQuery(String input)
    {

        return "{\n" +
                "\"from\" : 0 , \"size\" : 10," +
                "\"query\": {\n" +
                "\"multi_match\" : {\n" +
                "\"query\" : \"" + input + "\",\n" +
                "\"fields\" : [ \"bodyText^0.01\" , \"descriptionMeta^3\" , \"keyWordsMeta^3\" , \"authorMeta\" , \"contentTypeMeta^2\" , \"title^5\" ]\n" +
                "}\n" +
                "}\n" +
                "}";
    }


}
