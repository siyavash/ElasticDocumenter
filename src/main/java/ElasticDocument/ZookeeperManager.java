package ElasticDocument;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.List;

public class ZookeeperManager {
    private CuratorFramework client;
    private String path;
    private String ipAdress;

    public ZookeeperManager(String hosts, String path, String ipAddress) throws IOException, InterruptedException {
        this.path = path;
        this.ipAdress = ipAddress;
        client = CuratorFrameworkFactory.newClient(hosts, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    public void close() {
        client.close();
    }

    public void makeEphemeral() throws Exception {
        if (client.checkExists().forPath(path) == null)
            client.create().forPath(path);
        if (client.checkExists().forPath(path + "/servers") == null)
            client.create().forPath(path + "/servers");
        if (client.checkExists().forPath(path + "/servers/" + ipAdress) == null)
            client.create().forPath(path + "/servers/" + ipAdress);
        if (client.checkExists().forPath(path + "/servers/" + ipAdress + "/time") == null)
            client.create().forPath(path + "/servers/" + ipAdress + "/time");

        client.create().withMode(CreateMode.EPHEMERAL).forPath(path + "/servers/" + ipAdress + "/isAlive");
    }

    public long getNewTime(long timeSteps) throws Exception {
        InterProcessMutex lock = new InterProcessMutex(client, path + "/servers/" + ipAdress + "time/lock");
        lock.acquire();

        long time;

        time = Long.parseLong(new String(client.getData().forPath(path + "/servers/" + ipAdress + "/time")));

        lock.release();

        if (time != -1)
            return time;

        List<String> children = client.getChildren().forPath(path + "/servers/");
        for (String child : children) {
            if (client.checkExists().forPath(path + "/servers/" + child + "/isAlive") == null) {
                lock = new InterProcessMutex(client, path + "/servers/" + child + "time/lock");
                lock.acquire();
                time = Long.parseLong(new String(client.getData().forPath(path + "/servers/" + child + "/time")));
                if (time != -1) {
                    deleteTime(child);
                    lock.release();
                    return time;
                }
                lock.release();
            }
        }

        lock = new InterProcessMutex(client, path + "/nextTime/lock");
        lock.acquire();
        time = Long.parseLong(new String(client.getData().forPath(path + "/nextTime")));
        client.setData().forPath(path + "/nextTime", Long.toString(time + timeSteps).getBytes());
        lock.release();
        return time;
    }

    public void deleteTime() throws Exception {
        client.setData().forPath(path + "/servers/" + ipAdress + "/time", "-1".getBytes());
    }

    public void deleteTime(String child) throws Exception {
        client.setData().forPath(path + "/servers/" + child + "/time", "-1".getBytes());
    }
}
