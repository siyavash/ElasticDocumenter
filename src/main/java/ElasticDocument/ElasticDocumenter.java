package ElasticDocument;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticDocumenter
{
    private PageInfoDataStore dataStore;
    private Logger logger = Logger.getLogger(Class.class.getName());
    private ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue = new ArrayBlockingQueue<>(10000);
    private int iterateCount;

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
            startSendingRequestsThread(); //TODO close client
        } catch (InterruptedException e)
        {
            e.printStackTrace(); //TODO
        }
    }

    private void startSendingRequestsThread() throws InterruptedException
    {
        Thread slaveRequestingThread = new RequestingThread("slave", pageInfoArrayBlockingQueue);
        Thread masterRequestingThread = new RequestingThread("slave", pageInfoArrayBlockingQueue);

        slaveRequestingThread.start();
        masterRequestingThread.start();

        slaveRequestingThread.join();
        masterRequestingThread.join();
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
}
