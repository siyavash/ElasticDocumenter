package ElasticDocument;

import java.io.*;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticDocumenter
{
    private PageInfoDataStore dataStore;
    private ArrayBlockingQueue<PageInfo> pageInfoArrayBlockingQueue = new ArrayBlockingQueue<>(10000);
    private int iterateCount;

    public static void main(String[] args) throws IOException
    {
        Profiler.start();
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
            startSendingRequestsThread();
        } catch (InterruptedException e)
        {
            e.printStackTrace(); //TODO
        }
    }

    private void startSendingRequestsThread() throws InterruptedException
    {
        Thread slaveRequestingThread = new RequestingThread("slave", pageInfoArrayBlockingQueue);
        Thread masterRequestingThread = new RequestingThread("master", pageInfoArrayBlockingQueue);

        slaveRequestingThread.start();
        masterRequestingThread.start();

        slaveRequestingThread.join();
        masterRequestingThread.join();

        long startTime = 0;
        startTime = Long.parseLong(getDataFromFile("timeStamp.txt").split(",")[1]);
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
                Profiler.fatal("Failed to get iterator");
                System.exit(0);
            }
            PageInfo pageInfo;

            while ((pageInfo = pageInfoIterator.next()) != null)
            {
                try
                {
                    if (pageInfo.getNumOfInputLinks() == 0)
                    {
                        continue;
                    }
                    pageInfoArrayBlockingQueue.put(pageInfo);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                iterateCount++;


                if (iterateCount % 100 == 0)
                {
                    Profiler.info(iterateCount + " iteration done");
                }
            }

            try
            {
                PageInfo pageInfoFinsed = new PageInfo();
                pageInfoFinsed.setUrl("finished");
                pageInfoArrayBlockingQueue.put(pageInfoFinsed);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }).start();
    }

    private Iterator<PageInfo> findCorrectIterator() throws IOException
    {
        String lastCheckedURL;
        String timeStamps;
        lastCheckedURL = getDataFromFile("lastUrlName.txt");
        timeStamps = getDataFromFile("timeStamp.txt");
        Iterator<PageInfo> rowIterator = null;
        long start;
        long currentTime = System.currentTimeMillis() - (60000);
        String[] timeStampParts;
        try
        {
            timeStampParts = timeStamps.split(",");
        } catch (NullPointerException e)
        {
            timeStampParts = new String[0];
        }
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
        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;

        try
        {
            fileWriter = new FileWriter("timeStamp.txt");
            bufferedWriter = new BufferedWriter(fileWriter);

            Profiler.info("url name file found");
            bufferedWriter.write(start + "");
            if (end != -1)
                bufferedWriter.write("," + end);

        } catch (IOException e)
        {
            if (e instanceof FileNotFoundException)
            {
                Profiler.error("url name file not found");
                return;
            }

            e.printStackTrace();

        } finally
        {

            try
            {

                if (bufferedWriter != null)
                    bufferedWriter.close();

                if (fileWriter != null)
                    fileWriter.close();

            } catch (IOException ex)
            {
                ex.printStackTrace();
            }

        }
    }

    private String getDataFromFile(String fileName)
    {
        try (Scanner scanner = new Scanner(new File(fileName)))
        {
            return scanner.nextLine();
        } catch (FileNotFoundException e)
        {
            Profiler.info(fileName + " not found");
            return null;
        }
    }
}
