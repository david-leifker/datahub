package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Helpers for domain-separated writers: privilege selection by entity existence, and authorizing
 * writes while matching domain-scoped policies against <em>proposed</em> domains when the domains
 * aspect is being established.
 */
public final class DomainWriteAuthorizationUtils {

  private DomainWriteAuthorizationUtils() {}

  /**
   * Collect proposed {@code domains} aspects from a batch, keyed by entity URN. Later items for the
   * same URN overwrite earlier ones.
   */
  @Nonnull
  public static Map<Urn, Domains> extractProposedDomainsByUrn(
      @Nonnull Collection<? extends BatchItem> batchItems) {
    Map<Urn, Domains> proposed = new HashMap<>();
    for (BatchItem item : batchItems) {
      if (!DOMAINS_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }
      Domains domains = item.getAspect(Domains.class);
      if (domains == null && item.getRecordTemplate() != null) {
        domains = RecordUtils.toRecordTemplate(Domains.class, item.getRecordTemplate().data());
      }
      if (domains != null) {
        proposed.put(item.getUrn(), domains);
      }
    }
    return proposed;
  }

  @Nonnull
  public static Map<Urn, Boolean> resolveEntityExists(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    return aspectRetriever.entityExists(operationContext, Set.copyOf(urns));
  }

  @Nonnull
  public static Map<Urn, Boolean> resolveDomainsAspectExists(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    Map<Urn, Map<String, Aspect>> latest =
        aspectRetriever.getLatestAspectObjects(
            operationContext, Set.copyOf(urns), Set.of(DOMAINS_ASPECT_NAME));
    Map<Urn, Boolean> result = new HashMap<>();
    for (Urn urn : urns) {
      result.put(urn, latest.getOrDefault(urn, Map.of()).containsKey(DOMAINS_ASPECT_NAME));
    }
    return result;
  }

  /**
   * Whether this write should match domain-scoped policies using <em>proposed</em> domains (entity
   * create with domains in the batch, or first write establishing a missing domains aspect).
   */
  public static boolean shouldUseProposedDomainsForMatch(
      boolean entityExists, boolean domainsAspectExists, boolean batchProposesDomainsForUrn) {
    if (!batchProposesDomainsForUrn) {
      return false;
    }
    if (!entityExists) {
      return true;
    }
    return !domainsAspectExists;
  }

  /** Privilege path for an entity write given change type and existence. */
  @Nonnull
  public static com.linkedin.metadata.authorization.ApiOperation resolveApiOperation(
      @Nonnull ChangeType changeType, boolean entityExists) {
    switch (changeType) {
      case CREATE_ENTITY:
        return entityExists ? UPDATE : CREATE;
      case CREATE:
        return UPDATE;
      case UPSERT:
      case UPDATE:
      case RESTATE:
      case PATCH:
        return entityExists ? UPDATE : CREATE;
      case DELETE:
        return com.linkedin.metadata.authorization.ApiOperation.DELETE;
      default:
        return UPDATE;
    }
  }

  /**
   * Seed proposed domains from an MCP batch into the session resource-spec cache so domain-scoped
   * CREATE_ENTITY / EDIT policies can match at the API auth layer before ingest.
   */
  public static void seedProposedDomainsForApiAuth(
      @Nonnull AuthorizationSession session,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Collection<? extends BatchItem> batchItems) {
    if (!(session instanceof OperationContext) || batchItems.isEmpty()) {
      return;
    }
    seedProposedDomainsForApiAuth(
        (OperationContext) session,
        operationContext,
        aspectRetriever,
        extractProposedDomainsByUrn(batchItems));
  }

  /** Seed proposed domains extracted from raw MCPs (RestLi / Platform OpenAPI ingest path). */
  public static void seedProposedDomainsFromMcps(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<MetadataChangeProposal> mcps) {
    Map<Urn, Domains> proposed = extractProposedDomainsFromMcps(entityRegistry, mcps);
    if (proposed.isEmpty()) {
      return;
    }
    seedProposedDomainsForApiAuth(opContext, opContext, opContext.getAspectRetriever(), proposed);
  }

  private static void seedProposedDomainsForApiAuth(
      @Nonnull OperationContext opContext,
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever aspectRetriever,
      @Nonnull Map<Urn, Domains> proposedByUrn) {
    if (proposedByUrn.isEmpty()) {
      return;
    }
    Map<Urn, Boolean> entityExists =
        resolveEntityExists(operationContext, aspectRetriever, proposedByUrn.keySet());
    Map<Urn, Boolean> domainsExists =
        resolveDomainsAspectExists(operationContext, aspectRetriever, proposedByUrn.keySet());

    for (Map.Entry<Urn, Domains> entry : proposedByUrn.entrySet()) {
      Urn urn = entry.getKey();
      boolean exists = Boolean.TRUE.equals(entityExists.get(urn));
      boolean domainsAspectExists = Boolean.TRUE.equals(domainsExists.get(urn));
      if (!shouldUseProposedDomainsForMatch(exists, domainsAspectExists, true)) {
        continue;
      }
      EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());
      Set<Urn> domainUrns =
          EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(entry.getValue());
      Set<String> domainValues = expandDomainFieldValues(opContext, domainUrns);
      seedProposedDomainResourceSpec(opContext, resourceSpec, domainValues);
    }
  }

  @Nonnull
  public static Map<Urn, Domains> extractProposedDomainsFromMcps(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<MetadataChangeProposal> mcps) {
    Map<Urn, Domains> proposed = new HashMap<>();
    for (MetadataChangeProposal mcp : mcps) {
      if (mcp.getAspectName() == null
          || !DOMAINS_ASPECT_NAME.equals(mcp.getAspectName())
          || mcp.getAspect() == null) {
        continue;
      }
      Urn urn = mcp.getEntityUrn();
      if (urn == null) {
        try {
          urn =
              EntityKeyUtils.getUrnFromProposal(
                  mcp, entityRegistry.getEntitySpec(mcp.getEntityType()).getKeyAspectSpec());
        } catch (Exception e) {
          continue;
        }
      }
      try {
        AspectSpec aspectSpec =
            entityRegistry.getEntitySpec(mcp.getEntityType()).getAspectSpec(DOMAINS_ASPECT_NAME);
        if (aspectSpec == null) {
          continue;
        }
        RecordTemplate template =
            GenericRecordUtils.deserializeAspect(
                mcp.getAspect().getValue(), mcp.getAspect().getContentType(), aspectSpec);
        proposed.put(urn, RecordUtils.toRecordTemplate(Domains.class, template.data()));
      } catch (Exception e) {
        // Skip malformed domains aspects; auth will see empty proposed domains.
      }
    }
    return proposed;
  }

  /**
   * Authorize a write against the entity resource, optionally seeding the session resource-spec
   * cache with proposed domains (plus ancestors) so domain-scoped policies can match.
   */
  public static boolean isAuthorizedEntityWrite(
      @Nonnull AuthorizationSession session,
      @Nonnull Urn urn,
      @Nonnull com.linkedin.metadata.authorization.ApiOperation apiOperation,
      boolean useProposedDomains,
      @Nullable Domains proposedDomains) {

    EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), urn.toString());

    if (useProposedDomains && session instanceof OperationContext) {
      OperationContext opContext = (OperationContext) session;
      Set<Urn> domainUrns = EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(proposedDomains);
      Set<String> domainValues = expandDomainFieldValues(opContext, domainUrns);
      seedProposedDomainResourceSpec(opContext, resourceSpec, domainValues);
    }

    return AuthUtil.isAuthorized(
        session,
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupAPIPrivilege(ENTITY, apiOperation, urn.getEntityType())),
        resourceSpec);
  }

  @Nonnull
  public static Set<String> expandDomainFieldValues(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> domainUrns) {
    if (domainUrns.isEmpty()) {
      return Set.of();
    }
    Set<Urn> withAncestors =
        BoundHierarchyAccess.expandAncestors(
            opContext, HierarchyBindings.domainSpec(opContext), new HashSet<>(domainUrns));
    return withAncestors.stream().map(Urn::toString).collect(Collectors.toSet());
  }

  /**
   * Pre-populate the request-scoped resolved-spec cache so DOMAIN field matching uses proposed
   * values instead of a persisted (empty) domains aspect.
   */
  public static void seedProposedDomainResourceSpec(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec resourceSpec,
      @Nonnull Set<String> domainFieldValues) {
    Map<EntityFieldType, FieldResolver> resolvers = new EnumMap<>(EntityFieldType.class);
    resolvers.put(
        EntityFieldType.TYPE, FieldResolver.getResolverFromValues(Set.of(resourceSpec.getType())));
    resolvers.put(
        EntityFieldType.RESOURCE_TYPE,
        FieldResolver.getResolverFromValues(Set.of(resourceSpec.getType())));
    resolvers.put(
        EntityFieldType.URN, FieldResolver.getResolverFromValues(Set.of(resourceSpec.getEntity())));
    resolvers.put(
        EntityFieldType.RESOURCE_URN,
        FieldResolver.getResolverFromValues(Set.of(resourceSpec.getEntity())));
    resolvers.put(EntityFieldType.DOMAIN, FieldResolver.getResolverFromValues(domainFieldValues));
    opContext
        .getAuthorizationContext()
        .getSessionResourceSpecCache()
        .put(resourceSpec, new ResolvedEntitySpec(resourceSpec, resolvers));
  }
}
