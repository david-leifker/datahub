package com.linkedin.datahub.upgrade.buildindices;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.ResizeRequest;

import static com.linkedin.datahub.upgrade.buildindices.IndexUtils.*;


@RequiredArgsConstructor
@Slf4j
public class PreConfigureESStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents _esComponents;
  private final EntityRegistry _entityRegistry;

  @Override
  public String id() {
    return "PreConfigureESStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        // Get indices to update
        List<String> indexNames = getAllIndexNames(_esComponents, _entityRegistry);

        // Set refresh interval
        String refreshIntervalSeconds = "60s";
        for (String indexName : indexNames) {
          UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
          Map<String, Object> indexSettings = ImmutableMap.of("index.refresh_interval", refreshIntervalSeconds);

          request.settings(indexSettings);
          boolean ack = _esComponents.getSearchClient().indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
          log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexName, indexSettings, ack);
          if (!ack) {
            log.error("Partial index settings update, please validate current settings for refresh interval: {} Original setting: {}",
                indexNames, _esComponents.getIndexBuilder().getRefreshIntervalSeconds());
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // Clone indices
          String clonedName = indexName + "_clone_" + System.currentTimeMillis();
          ResizeRequest resizeRequest = new ResizeRequest(indexName, clonedName);
          boolean cloneAck = _esComponents.getSearchClient().indices().clone(resizeRequest, RequestOptions.DEFAULT).isAcknowledged();
          log.info("Cloned index {} into {}, Acknowledged: {}", indexName, clonedName, cloneAck);
          if (!cloneAck) {
            log.error("Partial index settings update, cloned indices may need to be cleaned up: {}", clonedName);
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }
        }
      } catch (Exception e) {
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
