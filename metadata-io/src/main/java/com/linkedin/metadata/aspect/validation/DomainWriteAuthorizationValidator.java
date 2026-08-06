package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.DomainWriteAuthorizationUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Authorizes establishing the {@code domains} aspect for domain-separated writers.
 *
 * <p>When an entity is new or has no domains aspect yet, domain-scoped Create Entity / Edit Entity
 * policies are evaluated against the <em>proposed</em> domains in the batch. Ongoing edits to
 * entities that already have domains keep using persisted domain matching at the API layer.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DomainWriteAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    List<BatchItem> domainsItems =
        items.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    if (domainsItems.isEmpty()) {
      return List.of();
    }

    Set<Urn> urns = domainsItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Boolean> entityExists =
        DomainWriteAuthorizationUtils.resolveEntityExists(operationContext, aspectRetriever, urns);
    Map<Urn, Boolean> domainsExists =
        DomainWriteAuthorizationUtils.resolveDomainsAspectExists(
            operationContext, aspectRetriever, urns);
    Map<Urn, Domains> proposedDomainsByUrn =
        DomainWriteAuthorizationUtils.extractProposedDomainsByUrn(batchItems);

    List<AspectValidationException> failures = new ArrayList<>();
    for (BatchItem item : domainsItems) {
      Urn urn = item.getUrn();
      boolean exists = Boolean.TRUE.equals(entityExists.get(urn));
      boolean domainsAspectExists = Boolean.TRUE.equals(domainsExists.get(urn));
      boolean batchProposesDomains = proposedDomainsByUrn.containsKey(urn);

      boolean useProposed =
          DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(
              exists, domainsAspectExists, batchProposesDomains);

      ApiOperation apiOperation =
          DomainWriteAuthorizationUtils.resolveApiOperation(item.getChangeType(), exists);

      boolean allowed =
          DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
              session,
              urn,
              apiOperation,
              useProposed,
              useProposed ? proposedDomainsByUrn.get(urn) : null);

      if (!allowed) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to "
                    + (exists ? "edit" : "create")
                    + " domains on entity "
                    + urn
                    + (useProposed
                        ? " (proposed domain does not match domain-scoped write policy)"
                        : "")));
      }
    }
    return failures;
  }
}
