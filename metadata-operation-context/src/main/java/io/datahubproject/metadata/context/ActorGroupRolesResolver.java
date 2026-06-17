package io.datahubproject.metadata.context;

import com.linkedin.common.urn.Urn;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;

/** Resolves roles inherited from group membership for a session actor. */
public interface ActorGroupRolesResolver {

  @Nonnull
  Set<Urn> fetchRolesViaGroups(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> groupUrns);
}
