/**
 * MainExecutor.java
 * @description:
 * @author: zhangkan@jd.com
 */
package com.jd.bdp.magpie.eggs;

import com.jd.bdp.magpie.Topology;

/**
 *
 */
public class MainExecutor {
	public static void main(String[] args) {
		Handler handler = new Handler();
		Topology topology = new Topology(handler);
		topology.run();
	}
}
