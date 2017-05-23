package org.apache.hadoop.mapred;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.CharsetUtil;
import org.mortbay.jetty.HttpHeaders;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * http://localhost:8888/mapOutput?job=job_1404091756589_0272&reduce=4&map=attempt_1404091756589_0272_1_05_000094_0_10023
 */
public class ShuffleHandler {
  private static final Log LOG = LogFactory.getLog(ShuffleHandler.class);

  public static final String CONNECTION_CLOSE = "close";
  boolean connectionKeepAliveEnabled = true;


  private static final Map<String, String> userRsrc =
      new ConcurrentHashMap<String, String>();

  private int port;
  private ChannelFactory selector;
  private final ChannelGroup accepted = new DefaultChannelGroup();
  protected HttpPipelineFactory pipelineFact;
  private int maxShuffleConnections = 10000;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
      "^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$",
      Pattern.CASE_INSENSITIVE);

  private final String baseDir;

  private Configuration conf;

  public ShuffleHandler(
      String baseDir) throws Exception {
    this.baseDir = baseDir;
    conf = new Configuration();
    System.out.println("Init part..");
    serviceInit(conf);
    serviceStart();
  }

  protected void serviceStart() {
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
    ServerBootstrap bootstrap = new ServerBootstrap(selector);
    try {
      pipelineFact = new HttpPipelineFactory(conf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.setPipelineFactory(pipelineFact);
    port = 8888;
    System.out.println("Port : " + port);
    Channel ch = bootstrap.bind(new InetSocketAddress(port));
    accepted.add(ch);
    port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
    pipelineFact.SHUFFLE.setPort(port);
    System.out.println("Listening on port " + port);
  }

  protected void serviceInit(Configuration conf) throws Exception {
    ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("ShuffleHandler Netty Worker #%d")
        .build();

    System.out.println("Creating selector");
    selector = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(bossFactory),
        Executors.newCachedThreadPool(workerFactory));

    System.out.println("Done with service Init");
  }

  protected Shuffle getShuffle(Configuration conf) {
    return new Shuffle(conf);
  }

  class HttpPipelineFactory implements ChannelPipelineFactory {

    final Shuffle SHUFFLE;

    public HttpPipelineFactory(Configuration conf) throws Exception {
      SHUFFLE = getShuffle(conf);
    }

    public void destroy() {
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      pipeline.addLast("decoder", new HttpRequestDecoder());
      pipeline.addLast("aggregator", new HttpChunkAggregator(1 << 16));
      pipeline.addLast("encoder", new HttpResponseEncoder());
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", SHUFFLE);
      return pipeline;
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }

  }

  class Shuffle extends SimpleChannelUpstreamHandler {

    private final Configuration conf;
    private final IndexCache indexCache;
    private int port;
    private int mapOutputMetaInfoCacheSize = 1000;
    private int connectionKeepAliveTimeOut = 5;

    public Shuffle(Configuration conf) {
      this.conf = conf;
      indexCache = new IndexCache(new JobConf(conf));
      this.port = 8888;
    }

    public void setPort(int port) {
      this.port = port;
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<String>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt)
        throws Exception {
      if ((maxShuffleConnections > 0) && (accepted.size()
          >= maxShuffleConnections)) {
        LOG.info(
            String.format("Current number of shuffle connections (%d) is " +
                    "greater than or equal to the max allowed shuffle connections (%d)",
                accepted.size(), maxShuffleConnections));
        evt.getChannel().close();
        return;
      }
      accepted.add(evt.getChannel());
      super.channelOpen(ctx, evt);

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt)
        throws Exception {
      HttpRequest request = (HttpRequest) evt.getMessage();
      if (request.getMethod() != GET) {
        sendError(ctx, METHOD_NOT_ALLOWED);
        return;
      }
      // Check whether the shuffle version is compatible
      /*
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
          request.getHeader(ShuffleHeader.HTTP_HEADER_NAME))
          || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
          request.getHeader(ShuffleHeader.HTTP_HEADER_VERSION))) {
        sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
      }
      */

      final Map<String, List<String>> q =
          new QueryStringDecoder(request.getUri()).getParameters();
      final List<String> mapIds = splitMaps(q.get("map"));
      final List<String> reduceQ = q.get("reduce");
      final List<String> jobQ = q.get("job");
      if (LOG.isDebugEnabled()) {
        LOG.debug("RECV: " + request.getUri() +
            "\n  mapId: " + mapIds +
            "\n  reduceId: " + reduceQ +
            "\n  jobId: " + jobQ);
      }

      if (mapIds == null || reduceQ == null || jobQ == null) {
        sendError(ctx, "Required param job, map and reduce", BAD_REQUEST);
        return;
      }
      if (reduceQ.size() != 1 || jobQ.size() != 1) {
        sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
        return;
      }
      int reduceId;
      String jobId;
      try {
        reduceId = Integer.parseInt(reduceQ.get(0));
        jobId = jobQ.get(0);
      } catch (NumberFormatException e) {
        sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
        return;
      } catch (IllegalArgumentException e) {
        sendError(ctx, "Bad job parameter", BAD_REQUEST);
        return;
      }
      final String reqUri = request.getUri();
      if (null == reqUri) {
        // TODO? add upstream?
        sendError(ctx, FORBIDDEN);
        return;
      }
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      try {
        verifyRequest(jobId, ctx, request, response,
            new URL("http", "", this.port, reqUri));
      } catch (IOException e) {
        LOG.warn("Shuffle failure ", e);
        sendError(ctx, e.getMessage(), UNAUTHORIZED);
        return;
      }

      Channel ch = evt.getChannel();
      ch.write(response);
      // TODO refactor the following into the pipeline
      ChannelFuture lastMap = null;
      for (String mapId : mapIds) {
        try {
          lastMap =
              sendMapOutput(ctx, ch, userRsrc.get(jobId), jobId, mapId,
                  reduceId);
          if (null == lastMap) {
            sendError(ctx, NOT_FOUND);
            return;
          }
        } catch (IOException e) {
          LOG.error("Shuffle error :", e);
          StringBuffer sb = new StringBuffer(e.getMessage());
          Throwable t = e;
          while (t.getCause() != null) {
            sb.append(t.getCause().getMessage());
            t = t.getCause();
          }
          sendError(ctx, sb.toString(), INTERNAL_SERVER_ERROR);
          return;
        }
      }
      lastMap.addListener(ChannelFutureListener.CLOSE);
    }

    protected void verifyRequest(String appid, ChannelHandlerContext ctx,
        HttpRequest request, HttpResponse response, URL requestUri)
        throws IOException {
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(requestUri);
      // hash from the fetcher
      String urlHashStr =
          request.getHeader(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if (urlHashStr == null) {
        LOG.info("Missing header hash for " + appid);
        throw new IOException("fetcher cannot be authenticated");
      }
      if (LOG.isDebugEnabled()) {
        int len = urlHashStr.length();
        LOG.debug("verifying request. enc_str=" + enc_str + "; hash=..." +
            urlHashStr.substring(len - len / 2, len - 1));
      }
      // Put shuffle version into http header
      response.setHeader(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.setHeader(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    }

    protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
        String user, String jobId, String mapId, int reduce)
        throws IOException {
      // TODO replace w/ rsrc alloc
      // $x/$user/appcache/$appId/output/$mapId
      final String base = baseDir + "/" + jobId + "/output" + "/" + mapId;
      // Index file
      Path indexFileName = new Path(base,"file.out.index");
      // Map-output file
      Path mapOutputFileName = new Path(base, "file.out");
      final IndexRecord info =
          indexCache.getIndexInformation(mapId, reduce, indexFileName, user);
      final ShuffleHeader header =
          new ShuffleHeader(mapId, info.partLength, info.rawLength, reduce);
      final DataOutputBuffer dob = new DataOutputBuffer();
      header.write(dob);
      ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
      final File spillfile = new File(mapOutputFileName.toString());
      RandomAccessFile spill;
      try {
        spill = SecureIOUtils.openForRandomRead(spillfile, "r", user, null);
      } catch (FileNotFoundException e) {
        LOG.info(spillfile + " not found");
        return null;
      }
      ChannelFuture writeFuture = null;
      if (ch.getPipeline().get(SslHandler.class) == null) {
        final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
            info.startOffset, info.partLength, true, 4096,
            readaheadPool, spillfile.getAbsolutePath());
        writeFuture = ch.write(partition);
        writeFuture.addListener(new ChannelFutureListener() {
          // TODO error handling; distinguish IO/connection failures,
          //      attribute to appropriate spill output
          @Override
          public void operationComplete(ChannelFuture future) {
            if (future.isSuccess()) {
              partition.transferSuccessful();
            }
            partition.releaseExternalResources();
          }
        });
      }
      return writeFuture;
    }

    protected void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    protected void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
      // Put shuffle version into http header
      response.setHeader(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.setHeader(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      response.setContent(
          ChannelBuffers.copiedBuffer(message, CharsetUtil.UTF_8));

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();
      if (cause instanceof TooLongFrameException) {
        sendError(ctx, BAD_REQUEST);
        return;
      } else if (cause instanceof IOException) {
        if (cause instanceof ClosedChannelException) {
          LOG.debug("Ignoring closed channel error", cause);
          return;
        }
        String message = String.valueOf(cause.getMessage());
        if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
          LOG.debug("Ignoring client socket close", cause);
          return;
        }
      }

      LOG.error("Shuffle error: ", cause);
      if (ch.isConnected()) {
        LOG.error("Shuffle error " + e);
        sendError(ctx, INTERNAL_SERVER_ERROR);
      }
    }

  }


  private static void printUsage() {
    StringBuilder sb = new StringBuilder();
    sb.append("java -cp  ./target/*:./target/lib/*: org.apache.hadoop.mapred" +
        ".ShuffleHandler <base_dir_where_shuffle_out_needs_to_be_generated> " +
        "<number_of_partitions_per_map_output_file> " +
        "<number_of_keys_per_partition>" +
        "<number_of_map_output_files>" +
        "\n");
    sb.append("e.g java -cp ./target/*:./target/lib/*: org.apache.hadoop.mapred" +
        ".ShuffleHandler /grid/4/ShuffleHandler/user_cache 1000 10000 100\n");
    System.out.println(sb.toString());
  }
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      printUsage();
      System.exit(-1);
    }
    String baseDir = args[0];
    int partitions = Integer.parseInt(args[1]);
    int keysPerPartition = Integer.parseInt(args[2]); //Keys per partition
    int attempts = Integer.parseInt(args[3]); //Number of map outputs


    ShuffleDataGenerator generator = new ShuffleDataGenerator
        ("job_1404091756589_0272",
            baseDir, partitions, keysPerPartition);
    generator.generate(attempts);

    System.out.println("Completed data generation..");

    ShuffleHandler handler = new ShuffleHandler(baseDir);

  }

}
