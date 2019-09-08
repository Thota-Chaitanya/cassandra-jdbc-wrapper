package com.github.adejanovski.cassandra.jdbc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class BuildCluster {

  private final static Logger logger = LoggerFactory.getLogger(BuildCluster.class);

  static String HOST = System.getProperty("host", ConnectionDetails.getHost());
  private static CCMBridge ccmBridge = null;
  private static Cluster cluster = null;
  private static Session session = null;
  private static boolean dynamicCluster = false;

  @BeforeSuite(groups = {"init"})
  public static void setUpBeforeSuite() {
    System.setProperty("cassandra.version", "3.0.4");
    System.setProperty("ipprefix", "127.0.0.");
    if (!isClusterActive()) {
      ccmBridge = CCMBridge.create("jdbc_cluster" + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()), 1, 1);
      ccmBridge.waitForUp(1);
      HOST = CCMBridge.ipOfNode(1);
      dynamicCluster = true;
    } else {
      HOST = "127.0.0.1";
    }

  }


  @AfterSuite(groups = {"init"})
  public static void tearDownAfterSuite() {
    System.out.println("CLOSING CASSANDRA CONNECTION");
    if (dynamicCluster) {
      System.out.println("Stopping nodes");
      try {
        ccmBridge.forceStop();
        System.out.println("Discarding cluster");
        ccmBridge.remove();
        HOST = System.getProperty("host", ConnectionDetails.getHost());
      } catch (Exception e) {
        System.out.println("Silent error discarding cluster");
      }
    }
  }


  private static boolean isClusterActive() {
    try {
      Builder builder = Cluster.builder()
          .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL));
      cluster = builder.addContactPoint("127.0.0.1").build();
      session = cluster.connect();
      return true;
    } catch (Exception e) {
      return false;
    }

  }

}
