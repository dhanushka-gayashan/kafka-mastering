package com.dhanushka.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GitHubSourceConnector extends SourceConnector {

  private static Logger log = LoggerFactory.getLogger(GitHubSourceConnector.class);
  private GitHubSourceConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> map) {
    config = new GitHubSourceConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return GitHubSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    // Define the individual task configurations that will be executed.
    ArrayList<Map<String, String>> configs = new ArrayList<>(1);
    configs.add(config.originalsStrings());
    return configs;
  }

  @Override
  public void stop() {
    // Do things that are necessary to stop your connector. (like Close DB Connections)
    // nothing is necessary to stop for this connector
  }

  @Override
  public ConfigDef config() {
    return GitHubSourceConnectorConfig.config();
  }
}
