import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;

public class ElasticDocumenter
{
    private PageInfoDataStore dataStore;

    public static void main(String[] args) throws IOException
    {
        PageInfoDataStore pageInfoDataStore = new PageInfoDataStore("2181", "localmaster,localslave");
        ElasticDocumenter elasticDocumenter = new ElasticDocumenter(pageInfoDataStore);

        elasticDocumenter.addDocuments();
    }

    public ElasticDocumenter(PageInfoDataStore dataStore)
    {
        this.dataStore = dataStore;
    }

    public ElasticDocumenter()
    {
    }

    public void addDocuments() throws IOException
    {
        String lastCheckedURL = findLastURL();
        Iterator<PageInfo> pageInfoIterator = dataStore.getRowIterator(lastCheckedURL);
        if (lastCheckedURL == null)
        {
            pageInfoIterator.next();
        }

        Gson gson = new Gson();
        PageInfo pageInfo;

        RestClient restClient = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http"),
                new HttpHost("127.0.0.1", 9201, "http")).build();


        while ((pageInfo = pageInfoIterator.next()) != null)
        {

            writeURLToFile(pageInfo.getUrl());
            String requestBody = gson.toJson(pageInfo, PageInfo.class);
            HttpEntity putEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
            Response addingResponse = restClient.performRequest("POST", "/gagoole/page/"/* + pageInfo.getUrl()*/, Collections.emptyMap(), putEntity);

        }

        restClient.close();
    }

    private String findLastURL()
    {
        BufferedReader br = null;
        FileReader fr = null;

        try
        {
            fr = new FileReader("lastUrlName.txt");
            br = new BufferedReader(fr);

            return br.readLine();

        } catch (IOException e)
        {
            if (e instanceof FileNotFoundException)
            {
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

        } catch (IOException e)
        {

            e.printStackTrace();

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
