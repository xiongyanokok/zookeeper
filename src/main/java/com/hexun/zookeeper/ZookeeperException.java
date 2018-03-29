package com.hexun.zookeeper;

/**
 * 注册中心异常
 * 
 * @author xiongyan
 * @date 2017年4月17日 下午5:01:04
 */
public final class ZookeeperException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    public ZookeeperException(final Exception cause) {
        super(cause);
    }
    
}
