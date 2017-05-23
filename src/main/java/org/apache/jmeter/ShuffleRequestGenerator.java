package org.apache.jmeter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Shuffle request generator plugin for jmeter
 */
public class ShuffleRequestGenerator extends AbstractJavaSamplerClient
    implements Serializable {

  String baseURL;
  RequestGenerator generator;

  // set up default arguments for the JMeter GUI
  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("host", "localhost");
    defaultParameters.addArgument("port", "8888");
    defaultParameters.addArgument("jobId", "job_1404091756589_0272");
    //Provide number of mapIds (batch) to fetch
    defaultParameters.addArgument("mapIds", "15");
    defaultParameters.addArgument("reducerId", "3");
    return defaultParameters;
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    int mapIdRange = context.getIntParameter("mapIds");
    if (mapIdRange <= 0) {
      throw new IllegalArgumentException("Wrong mapId..Please specify valid " +
          "id");
    }
    Set<String> mapIdSet = new HashSet<String>();
    for (int i = 0; i < mapIdRange; i++) {
      mapIdSet.add("attempt_" + i);
    }

    SampleResult result = new SampleResult();
    result.sampleStart(); // start stopwatch
    try {
      /*
      System.out.println("Host : " + context.getParameter("host"));
      System.out.println("port : " + context.getParameter("port"));
      System.out.println("jobId : " + context.getParameter("jobId"));
      System.out.println("reducerId : " + context.getParameter("reducerId"));
      System.out.println("mapIds : " + context.getParameter("mapIds"));
      System.out.println("mapIds : " + mapIdSet);
      */
      RequestGenerator generator = new RequestGenerator(context.getParameter
          ("host"), context.getIntParameter("port"),
          context.getParameter("jobId"), mapIdSet, context.getIntParameter
          ("reducerId"));
      long sTime = System.currentTimeMillis();
      generator.connect(RequestGenerator.CONNECTION_TIMEOUT);
      long eTime = System.currentTimeMillis();
      System.out.println("Connect time : " + (eTime - sTime));
      generator.downloadData();
      // In case we need to mess up downloading, uncomment following
      //generator.downloadData(true);
      generator.disconnect();
    } catch (IOException e) {
      e.printStackTrace();
      result.sampleEnd(); // stop stopwatch
      result.setSuccessful(false);
      result.setResponseMessage(e.getLocalizedMessage());
      return result;
    }
    result.sampleEnd(); // stop stopwatch
    result.setSuccessful(true);
    result.setResponseMessage("Success");
    return result;
  }
}
