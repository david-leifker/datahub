package com.linkedin.datahub.upgrade.buildindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.ArrayList;
import java.util.List;


public class BuildIndices implements Upgrade {

    private final List<UpgradeStep> _steps;

    public BuildIndices(final SystemMetadataService systemMetadataService, final TimeseriesAspectService timeseriesAspectService,
        final EntitySearchService entitySearchService, final GraphService graphService,
        final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
        final EntityRegistry entityRegistry) {
      _steps = buildSteps(systemMetadataService, timeseriesAspectService, entitySearchService, graphService,
          baseElasticSearchComponents, entityRegistry);
    }

    @Override
    public String id() {
      return "BuildIndices";
    }

    @Override
    public List<UpgradeStep> steps() {
      return _steps;
    }

    private List<UpgradeStep> buildSteps(final SystemMetadataService systemMetadataService, final TimeseriesAspectService
        timeseriesAspectService, final EntitySearchService entitySearchService, final GraphService graphService,
        final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
        final EntityRegistry entityRegistry) {
      final List<UpgradeStep> steps = new ArrayList<>();
      // Disable ES write mode/change refresh rate and clone indices
      steps.add(new PreConfigureESStep(baseElasticSearchComponents, entityRegistry));
      // Configure graphService, entitySearchService, systemMetadataService, timeseriesAspectService
      steps.add(new BuildIndicesStep(graphService, entitySearchService, systemMetadataService, timeseriesAspectService));
      // Reset configuration (and delete clones? Or just do this regularly? Or delete clone in pre-configure step if it already exists?
      steps.add(new PostBuildIndicesStep(baseElasticSearchComponents, entityRegistry));
      return steps;
    }

    @Override
    public List<UpgradeCleanupStep> cleanupSteps() {
      return ImmutableList.of();
    }

}
