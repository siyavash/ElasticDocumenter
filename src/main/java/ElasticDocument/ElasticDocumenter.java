package ElasticDocument;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class ElasticDocumenter {
    private PageInfoDataStore dataStore;
    private ArrayBlockingQueue<PageInfo> pageInfoFromHbase = new ArrayBlockingQueue<>(10000);
    private ArrayBlockingQueue<PageInfo> pageInfoToElastic = new ArrayBlockingQueue<>(10000);
    private int iterateCount;
    private ZookeeperManager zookeeperManager;
    private long timeSteps = 1000 * 60 * 20;
    private String ipAddress = InetAddress.getLocalHost().getHostAddress();
    private Thread iteratingThread;

    public static void main(String[] args) throws Exception {

        Profiler.start();

        PageInfoDataStore pageInfoDataStore =
                new PageInfoDataStore("2181", "master,slave");

        ElasticDocumenter elasticDocumenter
                = new ElasticDocumenter(pageInfoDataStore, "master:2181,slave:2181", "/elasticDocumenter");

        elasticDocumenter.startAddingDocuments();

        Profiler.close();
    }

    public ElasticDocumenter(PageInfoDataStore dataStore, String hosts, String path)
            throws Exception {
        this.dataStore = dataStore;
        zookeeperManager = new ZookeeperManager(hosts, path, ipAddress);

        zookeeperManager.makeEphemeral();
    }

    public void startAddingDocuments() throws Exception {

        long start = zookeeperManager.getNewTime(timeSteps);
        long end = start + timeSteps;
        Thread reindexDecider = new ReindexDecider(pageInfoFromHbase, pageInfoToElastic);
        reindexDecider.start();
        while (end < System.currentTimeMillis()) {
            startIteratingThread(start, end);

            startSendingRequestsThread();

            zookeeperManager.deleteTime();
            start = zookeeperManager.getNewTime(timeSteps);
            end = start + timeSteps;
            System.out.println(end + ">>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<");
        }

        zookeeperManager.close();
    }

    private void startSendingRequestsThread() throws InterruptedException {
        Thread masterRequestingThread = new RequestingThread("master", pageInfoFromHbase);
        Thread slaveRequestingThread = new RequestingThread("slave", pageInfoFromHbase);

        masterRequestingThread.start();
        slaveRequestingThread.start();

        masterRequestingThread.join();
        slaveRequestingThread.join();
    }

    private void startIteratingThread(long start, long end) throws IOException, InterruptedException {
        iteratingThread = new Thread(() -> {
            Iterator<PageInfo> pageInfoIterator = null;
            try {
                pageInfoIterator = dataStore.getRowIterator(start, end);
            } catch (IOException e) {
                Profiler.fatal("Failed to get iterator");
                e.printStackTrace();
                System.exit(87);
            }
            PageInfo pageInfo;

            while ((pageInfo = pageInfoIterator.next()) != null) {
                try {
                    if (pageInfo.getNumOfInputLinks() == 0 || pageInfo.getBodyText() == null) {
                        Profiler.info("skiped in iterating");
                        continue;
                    }
                    pageInfoFromHbase.put(pageInfo);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                iterateCount++;

                if (iterateCount % 100 == 0) {
                    Profiler.info(iterateCount + " iteration done");
                }
            }

            try {
                PageInfo pageInfoFinsed = new PageInfo();
                pageInfoFinsed.setUrl("finished");
                pageInfoFromHbase.put(pageInfoFinsed);
                pageInfoFromHbase.put(pageInfoFinsed);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        iteratingThread.start();
    }

}