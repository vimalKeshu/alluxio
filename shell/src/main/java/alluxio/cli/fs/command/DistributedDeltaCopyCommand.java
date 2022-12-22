package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.job.CmdConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies delta directory from source to target specified by args.
 */
@ThreadSafe
@PublicApi
public class DistributedDeltaCopyCommand extends AbstractDistributedJobCommand {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedDeltaCopyCommand.class);
  private static final String DEFAULT_FAILURE_FILE_PATH =
            "./logs/user/distributedDeltaCp_%s_failures.csv";

  private static final Option ACTIVE_JOB_COUNT_OPTION =
        Option.builder()
                .longOpt("active-jobs")
                .required(false)
                .hasArg(true)
                .numberOfArgs(1)
                .type(Number.class)
                .argName("active job count")
                .desc("Number of active jobs that can run at the same time. Later jobs must wait. "
                        + "The default upper limit is "
                        + AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS)
                .build();

  private static final Option OVERWRITE_OPTION =
        Option.builder()
                .longOpt("overwrite")
                .required(false)
                .hasArg(true)
                .numberOfArgs(1)
                .type(Boolean.class)
                .argName("overwrite")
                .desc("Whether to overwrite the destination. Default is true.")
                .build();

  private static final Option BATCH_SIZE_OPTION =
        Option.builder()
                .longOpt("batch-size")
                .required(false)
                .hasArg(true)
                .numberOfArgs(1)
                .type(Number.class)
                .argName("batch-size")
                .desc("Number of files per request")
                .build();

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedDeltaCopyCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedDeltaCp";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ACTIVE_JOB_COUNT_OPTION).addOption(OVERWRITE_OPTION)
                .addOption(BATCH_SIZE_OPTION).addOption(ASYNC_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public String getUsage() {
    return "distributedDeltaCp [--active-jobs <num>] [--batch-size <num>] <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies delta directory from source to target.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException  {
    mActiveJobs = FileSystemShellUtils.getIntArg(cl, ACTIVE_JOB_COUNT_OPTION,
                AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS);
    boolean overwrite = FileSystemShellUtils.getBoolArg(cl, OVERWRITE_OPTION, true);

    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);

    if (PathUtils.hasPrefix(dstPath.toString(), srcPath.toString())) {
      throw new RuntimeException(
              ExceptionMessage.MIGRATE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(srcPath, dstPath));
    }

    AlluxioConfiguration conf = mFsContext.getPathConf(dstPath);
    int defaultBatchSize = conf.getInt(PropertyKey.JOB_REQUEST_BATCH_SIZE);
    int batchSize = FileSystemShellUtils.getIntArg(cl, BATCH_SIZE_OPTION, defaultBatchSize);
    System.out.println("Please wait for command submission to finish..");

    ListStatusPOptions options = ListStatusPOptions
            .newBuilder()
            .setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions
                    .newBuilder()
                    .setSyncIntervalMs(0)
                    .build())
            .build();
    Map<AlluxioURI, AlluxioURI> deltaPaths =
            findDeltaFile(mFileSystem, options, srcPath, dstPath, new HashSet<>());
    Set<String> failures = new HashSet<>();
    deltaPaths.forEach((source, target) -> {
      long jobControlId = distributedSync(source, target, overwrite, batchSize);
      System.out.format("Delta source: %s", source);
      System.out.format("Delta target: %s", target);
      System.out.format("Submitted successfully, jobControlId = %s%n"
                    + "Waiting for the command to finish ...%n", Long.toString(jobControlId));
      waitForCmd(jobControlId);
      postProcessing(jobControlId);
      failures.addAll(getFailedFiles());
    });

    if (failures.size() > 0) {
      processFailures(args[0], failures, DEFAULT_FAILURE_FILE_PATH);
    }

    return failures.size();
  }

  private Long distributedSync(AlluxioURI srcPath, AlluxioURI dstPath,
                                 boolean overwrite, int batchSize) {
    CmdConfig cmdConfig = new MigrateCliConfig(srcPath.getPath(),
                dstPath.getPath(), WriteType.THROUGH, overwrite, batchSize);
    return submit(cmdConfig);
  }

  @Override
  public void close() throws IOException {
    mClient.close();
  }

  private Map<AlluxioURI, AlluxioURI> findDeltaFile(FileSystem fs,
                                                    ListStatusPOptions options,
                                                    AlluxioURI source,
                                                    AlluxioURI target,
                                                    Set<String> ignores)
          throws AlluxioException, IOException {
    Set<String> deltaPaths = new HashSet<>();

    Set<URIStatus> sourcePaths = new HashSet<>();
    fs.loadMetadata(source, options);
    browseFiles(fs, source, sourcePaths, ignores);

    Map<String, URIStatus> sourceDeltaPaths = sourcePaths
                .stream()
                .collect(Collectors
                        .toMap(uriStatus -> uriStatus
                                .getPath()
                                .replace(source.getPath(), ""), uriStatus -> uriStatus));

    if (fs.exists(target)) {
      Set<URIStatus> targetPaths = new HashSet<>();
      fs.loadMetadata(target, options);
      browseFiles(fs, target, targetPaths, ignores);
      Map<String, URIStatus> targetDeltaPaths = targetPaths
                    .stream()
                    .collect(Collectors
                            .toMap(uriStatus -> uriStatus
                                    .getPath()
                                    .replace(target.getPath(), ""), uriStatus -> uriStatus));
      for (String sPath: sourceDeltaPaths.keySet()) {
        if (!targetDeltaPaths.containsKey(sPath)
                || targetDeltaPaths.get(sPath).getLength()
                != sourceDeltaPaths.get(sPath).getLength()) {
          deltaPaths.add(new AlluxioURI(sourceDeltaPaths.get(sPath).getPath())
                  .getParent().getPath().replace(source.getPath(), ""));
        }
      }
    } else {
      deltaPaths.addAll(sourceDeltaPaths
                    .entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .map(uriStatus -> new AlluxioURI(uriStatus.getPath())
                            .getParent().getPath().replace(source.getPath(), ""))
                    .collect(Collectors.toSet()));
    }

    System.out.println("Delta files:");
    deltaPaths.forEach(System.out::println);

    return deltaPaths.stream().collect(Collectors
                .toMap(path -> new AlluxioURI(source, new AlluxioURI(path)),
                        path -> new AlluxioURI(target, new AlluxioURI(path))));
  }

  private void browseFiles(FileSystem fs,
                             AlluxioURI path,
                             Set<URIStatus> paths,
                             Set<String> ignores) throws AlluxioException, IOException {
    for (URIStatus uriStatus: fs.listStatus(path)) {
      if (isIgnore(uriStatus.getName(), ignores)) {
        continue;
      }
      if (uriStatus.isFolder()) {
        browseFiles(fs, new AlluxioURI(uriStatus.getPath()), paths, ignores);
      } else {
        paths.add(uriStatus);
      }
    }
  }

  private boolean isIgnore(String name, Set<String> ignores) {
    if (ignores == null || ignores.isEmpty()) {
      return false;
    }
    return ignores.stream().anyMatch(name::contains);
  }
}
