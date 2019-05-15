package test.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper
 * 连接zookeeper
 * 创建znode
 * 获取znode值
 * 断开链接
 */
public class ZookeeperDemo implements Watcher{
    private ZooKeeper zookeeper;
     /**
     * 超时时间
     */
    private static final int SESSION_TIME_OUT = 200 * 1000;
     private CountDownLatch countDownLatch = new CountDownLatch(1);


    /**
     * 链接zookeeper
     * @return
     * @throws IOException
     */
    public void zkConnect( ) throws IOException {
        //zookeeper的ip:端口
        String path = "192.168.1.70:2181,192.168.1.70:2182,192.168.1.70:2183";
        //第二个参数是超时时间，第三个参数是设置观察者,现在可以先不管
        zookeeper = new ZooKeeper(path,SESSION_TIME_OUT, this);
//        return zookeeper;
    }

    /**
     * 创建znode节点
     * @param path　znode的路径
     * @param value　znode的值
     * @param watcher
     * @param node //创建node的模式
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createZnode(String path, byte[] value, Watcher watcher, CreateMode node ) throws KeeperException, InterruptedException {
        zookeeper.create(path, value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 通过path获得znode的值
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getZnodeValue(String path ) throws KeeperException, InterruptedException {
        //第二个值是代表是否开启监听，这里还是先不管.第三个参数就是结构体
        byte[] data = zookeeper.getData(path, false, new Stat());
        return new String(data);
    }

    public void close() {
        try {
            if (zookeeper != null) {
                zookeeper.close();
            }
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZookeeperDemo zookeeperDemo = new ZookeeperDemo();
        //获取连接
        zookeeperDemo.zkConnect();
        String path = "/test2";  // 创建一个test2的文件 里面的内容是aaaa
        String value ="aaaa";
        
        //创建znode
        zookeeperDemo.createZnode(path, value.getBytes(), null, CreateMode.PERSISTENT);
        //获取znode的值  
        String znodeValue = zookeeperDemo.getZnodeValue("/test1"); //查询test1的值
        System.out.println(znodeValue);
        Thread.sleep(1000 * 60 * 50);
    }

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		
	}
}
