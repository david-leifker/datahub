package com.linkedin.metadata.authorization;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class DomainWriteAuthorizationUtilsTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn DOMAIN_X = UrnUtils.getUrn("urn:li:domain:engineering");

  @Test
  public void testShouldUseProposedDomains_newEntityWithDomains() {
    assertTrue(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(false, false, true));
  }

  @Test
  public void testShouldUseProposedDomains_newEntityWithoutDomains() {
    assertFalse(
        DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(false, false, false));
  }

  @Test
  public void testShouldUseProposedDomains_existingEntityMissingDomains() {
    assertTrue(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(true, false, true));
  }

  @Test
  public void testShouldUseProposedDomains_existingEntityWithDomains() {
    assertFalse(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(true, true, true));
  }

  @Test
  public void testResolveApiOperation_upsertNewEntityUsesCreate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.UPSERT, false),
        ApiOperation.CREATE);
  }

  @Test
  public void testResolveApiOperation_upsertExistingUsesUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.UPSERT, true),
        ApiOperation.UPDATE);
  }

  @Test
  public void testResolveApiOperation_createEntityOnExistingUsesUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.CREATE_ENTITY, true),
        ApiOperation.UPDATE);
  }

  @Test
  public void testResolveApiOperation_aspectCreateAlwaysUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.CREATE, false),
        ApiOperation.UPDATE);
  }

  @Test
  public void testExtractProposedDomainsByUrn() {
    Domains domains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    AspectSpec domainsSpec = mock(AspectSpec.class);
    when(domainsSpec.getName()).thenReturn("domains");
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");

    TestMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsSpec)
            .recordTemplate(domains)
            .changeType(ChangeType.CREATE_ENTITY)
            .build();

    Map<Urn, Domains> proposed =
        DomainWriteAuthorizationUtils.extractProposedDomainsByUrn(List.of(item));
    assertEquals(proposed.size(), 1);
    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(proposed.get(DATASET_URN)),
        Set.of(DOMAIN_X));
  }
}
