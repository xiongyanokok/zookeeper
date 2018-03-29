package com.hexun.zookeeper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

/**
 * zookeeper 注册中心.
 * 
 * @author xiongyan
 * @date 2017年4月17日 下午5:01:04
 */
@Slf4j
public class ZookeeperRegistryCenter extends ZookeeperConfiguration implements RegistryCenter {
	
	/**
	 * TreeCache
	 */
	private Map<String, TreeCache> caches = new HashMap<>();
	
    /**
     * Zookeeper framework client
     */
    private CuratorFramework client;
    
    /**
     * 销毁
     */
	public void destroy() {
    	for (Entry<String, TreeCache> each : caches.entrySet()) {
            each.getValue().close();
        }
    	waitForCacheClose();
        CloseableUtils.closeQuietly(client);
	}

    /**
     * 初始化
     */
	public void init() {
		log.debug("monitor: zookeeper registry center init, server lists is: {}.", this.getServers());
		Builder builder = CuratorFrameworkFactory.builder()
                .connectString(this.getServers())
                .retryPolicy(new ExponentialBackoffRetry(this.getBaseSleepTimeMilliseconds(), this.getMaxRetries(), this.getMaxSleepTimeMilliseconds()))
                .namespace(this.getNamespace());
        if (0 != this.getSessionTimeoutMilliseconds()) {
            builder.sessionTimeoutMs(this.getSessionTimeoutMilliseconds());
        }
        if (0 != this.getConnectionTimeoutMilliseconds()) {
            builder.connectionTimeoutMs(this.getConnectionTimeoutMilliseconds());
        }
        if (!Strings.isNullOrEmpty(this.getDigest())) {
            builder.authorization("digest", this.getDigest().getBytes(Charsets.UTF_8))
               .aclProvider(new ACLProvider() {
                   
                   @Override
                   public List<ACL> getDefaultAcl() {
                       return ZooDefs.Ids.CREATOR_ALL_ACL;
                   }
                   
                   @Override
                   public List<ACL> getAclForPath(final String path) {
                       return ZooDefs.Ids.CREATOR_ALL_ACL;
                   }
               });
        }
        client = builder.build();
        client.start();
        try {
            if (!client.blockUntilConnected(this.getMaxSleepTimeMilliseconds() * this.getMaxRetries(), TimeUnit.MILLISECONDS)) {
                client.close();
                throw new KeeperException.OperationTimeoutException();
            }
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
	}
	
    /**
     * 等待500ms, cache先关闭再关闭client, 否则会抛异常
     * 因为异步处理, 可能会导致client先关闭而cache还未关闭结束.
     */
    private void waitForCacheClose() {
        try {
            Thread.sleep(500L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public String get(final String key) {
        try {
            return new String(client.getData().forPath(key), Charsets.UTF_8);
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            List<String> result = client.getChildren().forPath(key);
            Collections.sort(result, (String o1, String o2) -> o2.compareTo(o1));
            return result;
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public boolean isExisted(final String key) {
        try {
            return null != client.checkExists().forPath(key);
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void persist(final String key, final String value) {
        try {
            if (!isExisted(key)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(key, value.getBytes(Charsets.UTF_8));
            } else {
                update(key, value);
            }
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void update(final String key, final String value) {
        try {
        	if (isExisted(key)) {
        		client.setData().forPath(key, value.getBytes(Charsets.UTF_8));
        	}
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void ephemeral(final String key, final String value) {
        try {
            if (isExisted(key)) {
                client.delete().deletingChildrenIfNeeded().forPath(key);
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(Charsets.UTF_8));
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public String persistSequential(final String key, final String value) {
        try {
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(key, value.getBytes(Charsets.UTF_8));
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void ephemeralSequential(final String key) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void remove(final String key) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(key);
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public void addCacheData(final String cachePath) {
        try {
        	TreeCache cache = new TreeCache(client, cachePath);
            cache.start();
            caches.put(cachePath + "/", cache);
        } catch (final Exception ex) {
        	throw new ZookeeperException(ex);
        }
    }
    
    @Override
    public TreeCache getCache(final String cachePath) {
        return caches.get(cachePath + "/");
    }
    
}
