package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_MEMBERSHIP_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.SessionActorIdentity;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ActorGroupMembershipFetcherTest {

  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn CORP_GROUP = UrnUtils.getUrn("urn:li:corpGroup:corp");
  private static final Urn NATIVE_GROUP = UrnUtils.getUrn("urn:li:corpGroup:native");
  private static final Urn ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");

  private EntityClient entityClient;
  private ActorGroupMembershipFetcher fetcher;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityClient = Mockito.mock(EntityClient.class);
    fetcher = new ActorGroupMembershipFetcher(entityClient);
    opContext = Mockito.mock(OperationContext.class);
  }

  @Test
  public void testFetchMergesAndDedupesGroups() throws Exception {
    final GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(new com.linkedin.common.UrnArray(CORP_GROUP, NATIVE_GROUP));

    final NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
    nativeGroupMembership.setNativeGroups(new com.linkedin.common.UrnArray(NATIVE_GROUP));

    final RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(new com.linkedin.common.UrnArray(ROLE));

    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(groupMembership.data())));
    aspectMap.put(
        NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())));
    aspectMap.put(
        ROLE_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(roleMembership.data())));

    when(entityClient.batchGetV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Set.of(USER_URN)),
            eq(
                ImmutableSet.of(
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                    ROLE_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(Map.of(USER_URN, new EntityResponse().setAspects(aspectMap)));

    final SessionActorIdentity identity = fetcher.fetch(opContext, USER_URN);

    assertEquals(identity.getGroups().size(), 2);
    assertTrue(identity.getGroups().contains(CORP_GROUP));
    assertTrue(identity.getGroups().contains(NATIVE_GROUP));
    assertEquals(identity.getDirectRoles(), Set.of(ROLE));
  }

  @Test
  public void testFetchEmptyWhenUserMissing() throws Exception {
    when(entityClient.batchGetV2(
            eq(opContext),
            eq(CORP_USER_ENTITY_NAME),
            eq(Set.of(USER_URN)),
            eq(
                ImmutableSet.of(
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                    ROLE_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(Map.of());

    final SessionActorIdentity identity = fetcher.fetch(opContext, USER_URN);

    assertTrue(identity.getGroups().isEmpty());
    assertTrue(identity.getDirectRoles().isEmpty());
  }
}
