package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.DomainWriteAuthorizationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainWriteAuthorizationValidatorTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn DOMAIN_X = UrnUtils.getUrn("urn:li:domain:engineering");

  private MockedStatic<DomainWriteAuthorizationUtils> domainWriteUtilsMock;
  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private AuthorizationSession session;
  private DomainWriteAuthorizationValidator validator;
  private com.linkedin.metadata.models.EntitySpec entitySpec;
  private AspectSpec domainsAspectSpec;

  @BeforeMethod
  public void setUp() {
    domainWriteUtilsMock = Mockito.mockStatic(DomainWriteAuthorizationUtils.class);
    // Call through for helpers the validator needs; authorize path is stubbed per test.
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveEntityExists(any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveDomainsAspectExists(any(), any(), any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.extractProposedDomainsByUrn(any()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(
                    anyBoolean(), anyBoolean(), anyBoolean()))
        .thenCallRealMethod();
    domainWriteUtilsMock
        .when(() -> DomainWriteAuthorizationUtils.resolveApiOperation(any(), anyBoolean()))
        .thenCallRealMethod();

    aspectRetriever = mock(AspectRetriever.class);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
    session = mock(AuthorizationSession.class);

    entitySpec = mock(com.linkedin.metadata.models.EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");
    domainsAspectSpec = mock(AspectSpec.class);
    when(domainsAspectSpec.getName()).thenReturn("domains");

    validator = new DomainWriteAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(DomainWriteAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(
                List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("ALL")
                        .aspectName("domains")
                        .build()))
            .build());
  }

  @AfterMethod
  public void tearDown() {
    domainWriteUtilsMock.close();
  }

  @Test
  public void testAllowsCreateEntityWithProposedDomain() {
    TestMCP item = domainsItem(ChangeType.CREATE_ENTITY);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    stubWriteAuth(true);

    assertEquals(validate(item).count(), 0);
  }

  @Test
  public void testDeniesCreateEntityWhenUnauthorized() {
    TestMCP item = domainsItem(ChangeType.CREATE_ENTITY);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, false));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    stubWriteAuth(false);

    assertEquals(validate(item).count(), 1);
  }

  @Test
  public void testAllowsAspectCreateWhenDomainsMissing() {
    TestMCP item = domainsItem(ChangeType.CREATE);
    when(aspectRetriever.entityExists(any(), eq(Set.of(DATASET_URN))))
        .thenReturn(Map.of(DATASET_URN, true));
    when(aspectRetriever.getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), any()))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    stubWriteAuth(true);

    assertEquals(validate(item).count(), 0);
    domainWriteUtilsMock.verify(
        () ->
            DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                eq(session), eq(DATASET_URN), eq(ApiOperation.UPDATE), eq(true), any()));
  }

  @Test
  public void testIgnoresNonDomainsAspects() {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    TestMCP ownership =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(ownershipSpec)
            .changeType(ChangeType.CREATE_ENTITY)
            .build();

    assertEquals(validate(ownership).count(), 0);
  }

  private void stubWriteAuth(boolean allowed) {
    domainWriteUtilsMock
        .when(
            () ->
                DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
                    any(), any(), any(), anyBoolean(), any()))
        .thenReturn(allowed);
  }

  private TestMCP domainsItem(ChangeType changeType) {
    Domains domains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    return TestMCP.builder()
        .urn(DATASET_URN)
        .entitySpec(entitySpec)
        .aspectSpec(domainsAspectSpec)
        .recordTemplate(domains)
        .changeType(changeType)
        .build();
  }

  private java.util.stream.Stream<
          com.linkedin.metadata.aspect.plugins.validation.AspectValidationException>
      validate(TestMCP item) {
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY, List.of(item), retrieverContext, session);
  }
}
