package io.datahubproject.metadata.context;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ServicesRegistryContext implements ContextInterface {

  @Nonnull private final RestrictedService restrictedService;

  @Nullable private final ActorGroupRolesResolver actorGroupRolesResolver;

  public Set<Urn> fetchRolesViaGroups(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> groupUrns) {
    if (actorGroupRolesResolver == null) {
      return Collections.emptySet();
    }
    return actorGroupRolesResolver.fetchRolesViaGroups(opContext, groupUrns);
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
