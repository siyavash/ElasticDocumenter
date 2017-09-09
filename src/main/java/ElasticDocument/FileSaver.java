package ElasticDocument;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class FileSaver {
    private ZooKeeper zookeeper;
    private String path;

    public FileSaver(String hosts, String path) throws IOException, InterruptedException {
        connectToZookeeper(hosts);
        this.path = path;
    }

    public String getZnodeData() throws KeeperException, InterruptedException {
        if (!znodeExistance())
            throw new NoNodeException();
        byte[] data = zookeeper.getData(path, false, null);
        return new String(data);
    }

    public void saveTimeStampsToZookeeper(long timeMid, long timeEnd, long timeDocumenterStarted)
            throws KeeperException, InterruptedException {
        creatZnode((timeMid + "," + timeEnd + "-" + timeDocumenterStarted).getBytes());
    }

    private void creatZnode(byte[] data) throws KeeperException, InterruptedException {
        zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private boolean znodeExistance() throws KeeperException, InterruptedException {
        return zookeeper.exists(path, true) != null;
    }

    private void connectToZookeeper(String hosts) throws IOException, InterruptedException {
        int numOfZookeepers = hosts.split(",").length;
        CountDownLatch connctedSignal = new CountDownLatch(numOfZookeepers);

        zookeeper = new ZooKeeper(hosts, 5000, event -> {
            if (event.getState() == KeeperState.SyncConnected) {
                connctedSignal.countDown();
            }
        });

        connctedSignal.await();
    }

    public void deleteZnode() throws KeeperException, InterruptedException {
        zookeeper.delete(path, zookeeper.exists(path, true).getVersion());
    }

    private void closeZookeeper() throws InterruptedException {
        zookeeper.close();
    }

    private void writeToTimeStampFile(long timeStart, long timeEnd, long timeDocumenterStarted) {

        try(FileWriter fileWriter = new FileWriter("timeStamp.txt");
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            Profiler.info("url name file found");
            bufferedWriter.write(timeStart + "");
            if (timeEnd != -1)
                bufferedWriter.write("," + timeEnd);

        } catch (IOException e) {
            if (e instanceof FileNotFoundException) {
                Profiler.error("url name file not found");
                return;
            }
            e.printStackTrace();
        }
    }

    private String getDataFromFile(String fileName) {
        try (Scanner scanner = new Scanner(new File(fileName))) {
            return scanner.nextLine();
        } catch (FileNotFoundException e) {
            Profiler.info(fileName + " not found");
            return null;
        }
    }
}
