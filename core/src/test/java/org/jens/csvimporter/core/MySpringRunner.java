package org.jens.csvimporter.core;

import org.jens.shorthand.spring.test.annotation.ShorthandTestSpring;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Jens Ritter on 24.08.2024.
 */
@ExtendWith(SpringExtension.class)
@ShorthandTestSpring(classes=MySpringRunner.SpringRunnerConfig.class)
public class MySpringRunner {

    @EnableAutoConfiguration
    @Configuration
    public static class SpringRunnerConfig {

        @Primary
        @Bean
        public EmbeddedDatabase ds() {
            return new EmbeddedDatabaseBuilder()
                .generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2)
                .build();
        }

    }

    @Test
    void testDI() {
        assertTrue(true);
    }
}
