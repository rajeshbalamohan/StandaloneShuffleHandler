package org.apache.jmeter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.IFile;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple shuffle request generator.
 * Connects to shuffle handler with certain mapIds and downloads the data
 */
public class RequestGenerator {

  HttpURLConnection connection;
  URL url;
  public final static int CONNECTION_TIMEOUT = 3 * 60 * 1000;
  final static int READ_TIMEOUT = 3 * 60 * 1000;
  boolean connectionSucceeed;
  Set<String> attemptIds;
  DataInputStream din;
  int reducerId;

  public RequestGenerator(String machine, int port,
      String jobId, Set<String> attemptIds, int reducerId) throws
      IOException {
    this.reducerId = reducerId;
    StringBuilder sb = new StringBuilder("http://" + machine + ":" + port +
        "/mapOutput?job=" + jobId +
        "&reduce=" + reducerId + "&map=");
    this.attemptIds = attemptIds;
    boolean first = true;
    for (String mapId : attemptIds) {
      if (first) {
        sb.append(mapId);
        first = false;
        continue;
      }
      sb.append(",");
      sb.append(mapId);
    }
    //Always use keepAlive
    sb.append("&keepAlive=true");
    url = new URL(sb.toString());
    //System.out.println("URL : " + url);
  }

  public boolean connect(int connectionTimeout) throws IOException {
    if (connection == null) {
      setupConnection();
    }
    validate();

    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout " + "[timeout = "
          + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = Math.min(CONNECTION_TIMEOUT, connectionTimeout);
    }
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    int connectionFailures = 0;
    while (true) {
      try {
        connection.connect();
        din = new DataInputStream(connection.getInputStream());
        break;
      } catch (IOException ioe) {
        // Don't attempt another connect if already cleanedup.
        // update the total remaining connect-timeout
        connectionTimeout -= unit;
        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (connectionTimeout == 0) {
          throw ioe;
        }
        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
        connectionFailures++;
      }
    }
    return true;
  }

  private void setupConnection() throws IOException {
    connection = (HttpURLConnection) url.openConnection();
    String jobTokenSecret = System.currentTimeMillis() + "";
    // generate hash of the url

    // put url hash into http header
    connection.addRequestProperty("UrlHash", "UrlHash");
    // set the read timeout
    connection.setReadTimeout(3 * 60 * 1000);
    connection.setConnectTimeout(3 * 60 * 1000);
    // put shuffle version into http header
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
  }

  private void validate() throws IOException {
    int rc = connection.getResponseCode();
    if (rc != HttpURLConnection.HTTP_OK) {
      throw new IOException("Got invalid response code " + rc + " from " + url
          + ": " + connection.getResponseMessage());
    }
    // get the shuffle version
    if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(connection
        .getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(connection
        .getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))) {
      throw new IOException("Incompatible shuffle response version");
    }
  }

  class Worker implements Runnable {
    public void run() {
     try {
       downloadData();
     } catch(IOException e) {
       e.printStackTrace();
     }
    }
  }

  public final AtomicBoolean greenFlag = new AtomicBoolean();

  public void downloadData(boolean viaThread) throws IOException {
    if (viaThread) {
      Thread t = new Thread(new Worker());
      t.start();
      while(!greenFlag.get()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      //interrupt intensionally to mess up
      t.interrupt();
    } else {
      downloadData();
    }
  }

  public void downloadData() throws IOException {
    for (String attemptId : attemptIds) {
      ShuffleHeader header = new ShuffleHeader();
      header.readFields(din);
      if (!header.getMapId().equalsIgnoreCase(attemptId)) {
        System.out.println("Wrong header received " + header.getMapId() + "; " +
            "url :" + url);
        throw new IllegalArgumentException(
            "Invalid header received: " + header.getMapId() + " partition: "
                + header.getPartition());
      }
      if (this.reducerId != header.getPartition()) {
        System.out.println(" data for the wrong partition map: " + attemptId +
            " len: "
            + header.getCompressedLength() + " decomp len: " + header
            .getUncompressedLength() + " for partition " + header
            .getPartition() + ", " +
            "expected partition: " + this.reducerId);
      }

      System.out.println("HEader : " + header.toString());

      byte[] b = new byte[(int) header.getCompressedLength()];
      // IOUtils.readFully(din, b, 0, b.length);
      CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
      CompressionCodec codec = codecFactory.getCodecByClassName(DefaultCodec
          .class.getName());

      b = new byte[(int) header.getUncompressedLength()];
      greenFlag.set(true);
      IFile.Reader.readToMemory(b, din, (int)header.getCompressedLength(), codec, false, 4096);
      System.out.println("Read..." + header.getCompressedLength());
      greenFlag.set(false);
    }
  }

  public void disconnect() throws IOException {
    try {
      if (din != null) {
        din.close();
      }
      /*
      if (connection != null) {
        connection.disconnect();
        connection = null;
      }
      */
    } catch(Exception e) {
      //ignore
    }
  }

  public static void main(String[] args) throws Exception {
    String host = "localhost";
    int port = 8888;
    String jobId = "job_1404091756589_0272";
    int reduceId = 2;
    Set<String> mapIds = new HashSet<String>();
    for (int i = 1; i < 10; i++) {
      mapIds.add("attempt_" + i);
    }
    /*
    RequestGenerator generator =
        new RequestGenerator("http://localhost:8888/mapOutput?job" +
            "=job_1404091756589_0272&map=attempt_1&reduce=1");
  */

    RequestGenerator generator = new RequestGenerator(host, port, jobId,
        mapIds, reduceId);
    generator.connect(CONNECTION_TIMEOUT);

    generator.downloadData();

  }
}
