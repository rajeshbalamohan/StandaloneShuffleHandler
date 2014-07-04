package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.IOException;
import java.util.Random;

/**
 * A simple data generator tool for Shuffle phase.
 */
public class ShuffleDataGenerator {

  FileSystem fs;
  JobConf conf;
  CompressionCodec codec;
  String baseDirLocation;
  int partitions;
  int keysPerPartition;
  String jobId;

  public ShuffleDataGenerator(String jobId, String baseDirLocation) throws
      IOException {
    this(jobId, baseDirLocation, 1, 1000);
  }

  /**
   * Constructor
   *
   * @param jobId
   * @param baseDirLocation
   * @param partitions
   * @param keysPerPartition
   * @throws IOException
   */
  public ShuffleDataGenerator(String jobId, String baseDirLocation,
      int partitions, int keysPerPartition) throws IOException {
    this.jobId = jobId;
    this.baseDirLocation = baseDirLocation;
    this.partitions = Math.max(1, partitions);
    this.keysPerPartition = Math.max(1, keysPerPartition);
    init();
  }

  private void init() throws IOException {
    conf = new JobConf();
    fs = FileSystem.getLocal(conf);
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    codec = codecFactory.getCodecByClassName(DefaultCodec
        .class.getName());
  }


  /**
   * Generate data for specific number of attempts
   *
   * @param numAttempts
   * @throws IOException
   */
  public void generate(int numAttempts) throws IOException {
    for (int i = 0; i < numAttempts; i++) {
      generate("attempt_" + i);
    }
  }

  /**
   * Generate data for a given attempt
   *
   * @param attemptId
   * @throws IOException
   */
  public void generate(String attemptId) throws IOException {
    //The output stream for the final single output file
    Path shuffleOutput = getFileOutputPathForAttempt(attemptId);
    Path fileOutPath = new Path(shuffleOutput, "file.out");
    Path fileIndexPath = new Path(shuffleOutput, "file.out.index");

    FSDataOutputStream finalOut = fs.create(fileOutPath, true, 4096);

    SpillRecord sr = new SpillRecord(partitions);
    Text key = new Text();
    Text value = new Text();
    try {
      Random rnd = new Random();
      for (int i = 0; i < partitions; i++) {
        long segmentStart = finalOut.getPos();
        IFile.Writer writer = new IFile.Writer<Text, Text>(conf, finalOut,
            Text.class, Text.class, codec, null);

        //keys per partition
        int keys = rnd.nextInt(keysPerPartition);
        for (int j = 0; j < keys; j++) {
          key.set("Key_part_" + i + "_" + j);
          value.set("Value_part" + i + "_" + j);
          writer.append(key, value);
        }
        writer.close();

        IndexRecord rec =
            new IndexRecord(
                segmentStart,
                writer.getRawLength(),
                writer.getCompressedLength());

        sr.putIndex(rec, i);
      }
      sr.writeToFile(fileIndexPath, conf);
    } finally {
      finalOut.close();
    }
  }

  public Path getFileOutputPathForAttempt(String attemptId) throws IOException {
    Path baseDir = new Path(baseDirLocation + "/" + jobId + "/output");
    Path shuffleOutput = new Path(baseDir, attemptId);
    fs.mkdirs(shuffleOutput);
    return shuffleOutput;
  }

  public static void main(String[] args) throws IOException {
    ShuffleDataGenerator generator = new ShuffleDataGenerator
        ("job_1404091756589_0272",
        "./user_cache", 100, 10000);
    generator.generate(10);
    System.out.println("Done..");
  }
}
