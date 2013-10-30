/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph;

import java.net.URI;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.giraph.examples.Giraphx;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class GiraphRunner implements Tool {
  private static final Logger LOG = Logger.getLogger(GiraphRunner.class);
  private Configuration conf;

  final String [][] requiredOptions =
      {{"w", "Need to choose the number of workers (-w)"},
       {"if", "Need to set inputformat (-if)"}};

  private Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("q", "quiet", false, "Quiet output");
    options.addOption("w", "workers", true, "Number of workers");
    options.addOption("if", "inputFormat", true, "Graph inputformat");
    options.addOption("of", "outputFormat", true, "Graph outputformat");
    options.addOption("ip", "inputPath", true, "Graph input path");
    options.addOption("op", "outputPath", true, "Graph output path");
    options.addOption("c", "combiner", true, "VertexCombiner class");
    options.addOption("wc", "workerContext", true, "WorkerContext class");
    options.addOption("aw", "aggregatorWriter", true, "AggregatorWriter class");
    options.addOption("cf", "cacheFile", true, "Files for distributed cache");
    
    options.addOption("si", "sourceId", true, "Source vertex id");
    options.addOption("vn", "vertexNum", true, "Total number of vertices");
    options.addOption("pa", "partitioner", true, "Type of partitioner");
    options.addOption("vj", "versionOfJob", true, "Used in Giraphx to select tGiraphx or dGiraphx");
    options.addOption("ms", "maxsuperstep", true, "Max number of supersteps");

    
    options.addOption("ca", "customArguments", true, "provide custom" +
        " arguments for the job configuration in the form:" +
        " <param1>=<value1>,<param2>=<value2> etc.");

    return options;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    String vertexClassName = args[0];
    if(LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    // Verify all the options have been provided
    for (String[] requiredOption : requiredOptions) {
      if(!cmd.hasOption(requiredOption[0])) {
        System.out.println(requiredOption[1]);
        return -1;
      }
    }

    int workers = Integer.parseInt(cmd.getOptionValue('w'));
    GiraphJob job = new GiraphJob(getConf(), "Giraph: " + vertexClassName);
    job.setVertexClass(Class.forName(vertexClassName));
    job.setVertexInputFormatClass(Class.forName(cmd.getOptionValue("if")));
    job.setVertexOutputFormatClass(Class.forName(cmd.getOptionValue("of")));

    if(cmd.hasOption("ip")) {
      FileInputFormat.addInputPath(job, new Path(cmd.getOptionValue("ip")));
    } else {
      LOG.info("No input path specified. Ensure your InputFormat does not " +
              "require one.");
    }

    if(cmd.hasOption("op")) {
      FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("op")));
    } else {
      LOG.info("No output path specified. Ensure your OutputFormat does not " +
              "require one.");
    }

    if (cmd.hasOption("c")) {
        job.setVertexCombinerClass(Class.forName(cmd.getOptionValue("c")));
    }

    if (cmd.hasOption("wc")) {
        job.setWorkerContextClass(Class.forName(cmd.getOptionValue("wc")));
    }

    if (cmd.hasOption("aw")) {
        job.setAggregatorWriterClass(Class.forName(cmd.getOptionValue("aw")));
    }

    if (cmd.hasOption("cf")) {
        DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
            job.getConfiguration());
      }

      if (cmd.hasOption("ca")) {
        Configuration jobConf = job.getConfiguration();
        for (String paramValue :
            Splitter.on(',').split(cmd.getOptionValue("ca"))) {
          String[] parts = Iterables.toArray(Splitter.on('=').split(paramValue),
              String.class);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Unable to parse custom " +
                " argument: " + paramValue);
          }
          if (LOG.isInfoEnabled()) {
            LOG.info("Setting custom argument [" + parts[0] + "] to [" +
                parts[1] + "]");
          }
          jobConf.set(parts[0], parts[1]);
        }
      }

    //job.setWorkerConfiguration(workers, workers, 100.0f);

    boolean isQuiet = !cmd.hasOption('q');
    
    String sourceId=null;
    if (cmd.hasOption("si")) {
        sourceId = cmd.getOptionValue("si");
    }
    job.getConfiguration().setLong(Giraphx.SOURCE_ID, Long.parseLong(sourceId));
  
    String workerNum=workers+"";
    job.setWorkerConfiguration(Integer.parseInt(workerNum),
        Integer.parseInt(workerNum),
        100.0f);

    String vertexNumStr=null;
    if (cmd.hasOption("vn")) {
    	vertexNumStr = cmd.getOptionValue("vn");
    }
	int vertexNum = Integer.parseInt(vertexNumStr);
	Log.info("vertexNum "+vertexNum);
	job.setVertexNumConfiguration(vertexNum);

	String versionofjob=null;
    if (cmd.hasOption("vj")) {
    	versionofjob = cmd.getOptionValue("vj");
    }
    job.getConfiguration().set(Giraphx.VERSION_OF_JOB_CONF, versionofjob);

    String maxsuperstepstr=null;
    if (cmd.hasOption("ms")) {
    	maxsuperstepstr = cmd.getOptionValue("ms");
    }
    job.getConfiguration().setInt(Giraphx.MAX_SUPERSTEPS_CONF,
    		Integer.parseInt(maxsuperstepstr));

    String partitioner = "mesh";
    if (cmd.hasOption("pa")) {
    	partitioner = cmd.getOptionValue("pa");
    }
    job.setPartitionerTypeConfiguration(partitioner);
    job.getConfiguration().set(Giraphx.PARTITIONER_TYPE,partitioner);
    
    return job.run(isQuiet) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GiraphRunner(), args));
  }
}
