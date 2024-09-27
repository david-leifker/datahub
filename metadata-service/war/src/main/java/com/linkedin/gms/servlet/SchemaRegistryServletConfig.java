package com.linkedin.gms.servlet;

import com.linkedin.gms.factory.common.ObjectMapperFactory;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryServiceFactory;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.schema.registry",
    })
// Surgically import minimal requirements, avoiding broader componentScan
@Import({
  ObjectMapperFactory.class,
  SchemaRegistryServiceFactory.class,
  TopicConventionFactory.class
})
@Configuration
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class SchemaRegistryServletConfig {}
