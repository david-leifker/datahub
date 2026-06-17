package com.datahub.authentication.group;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.ActorGroupRolesResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ActorGroupMembershipRolesResolver implements ActorGroupRolesResolver {

  private final ActorGroupMembershipFetcher membershipFetcher;

  @Override
  @Nonnull
  public Set<Urn> fetchRolesViaGroups(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> groupUrns) {
    return membershipFetcher.fetchRolesViaGroups(opContext, groupUrns);
  }
}
