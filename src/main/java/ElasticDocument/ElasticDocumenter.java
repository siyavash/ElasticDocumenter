package ElasticDocument;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Iterator;
import java.util.Scanner;
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

    private void startSendingRequestsThread() throws InterruptedException, FileNotFoundException
    {
        Thread slaveRequestingThread = new RequestingThread("slave", pageInfoArrayBlockingQueue);
        Thread masterRequestingThread = new RequestingThread("master", pageInfoArrayBlockingQueue);

        slaveRequestingThread.start();
        masterRequestingThread.start();

        slaveRequestingThread.join();
        masterRequestingThread.join();

        long startTime = Long.parseLong(getDataFromFile("timeStamp.txt").split(",")[1]);
        writeToTimeStampFile(startTime, -1);

    }

    private void startIteratingThread() throws IOException //TODO handle exce[ptions
    {
        new Thread(() -> {
            Iterator<PageInfo> pageInfoIterator = null;
            try
            {
                pageInfoIterator = findCorrectIterator();
            } catch (IOException e)
            {
                e.printStackTrace(); //TODO
            }
            PageInfo pageInfo;

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
        }).start();
    }

    private Iterator<PageInfo> findCorrectIterator() throws IOException
    {
        String lastCheckedURL = getDataFromFile("lastUrlName.txt");
        String timeStamps = getDataFromFile("timeStamp.txt");
        Iterator<PageInfo> rowIterator = null;
        long start;
        long currentTime = System.currentTimeMillis() - (60000);

        String[] timeStampParts = timeStamps.split(",");
        switch (timeStampParts.length)
        {
            case 0:
                writeToTimeStampFile(0L, currentTime);
                rowIterator = dataStore.getRowIterator(0L, currentTime, lastCheckedURL);
                break;
            case 1:
                start = Long.parseLong(timeStampParts[0]);
                writeToTimeStampFile(start, currentTime);
                rowIterator = dataStore.getRowIterator(start, currentTime, lastCheckedURL);
                break;
            case 2:
                long end = Long.parseLong(timeStampParts[1]);
                start = Long.parseLong(timeStampParts[0]);
                rowIterator = dataStore.getRowIterator(start, end, lastCheckedURL);
                break;
        }

        if (lastCheckedURL != null)
        {
            rowIterator.next();
        }

        return rowIterator;
    }

    private void writeToTimeStampFile(long start, long end)
    {
        BufferedWriter bw = null;
        FileWriter fw = null;

        try
        {
            fw = new FileWriter("timeStamp.txt");
            bw = new BufferedWriter(fw);

            logger.info("url name file found");
            bw.write(start + "");
            if (end != -1)
                bw.write("," + end);

        } catch (IOException e)
        {
            if (e instanceof FileNotFoundException)
            {
                logger.warn("url name file not found");
                return;
            }

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

    private String getDataFromFile(String fileName) throws FileNotFoundException
    {
        try (Scanner scanner = new Scanner(new File(fileName)))
        {
            return scanner.nextLine();
        }
    }
}
