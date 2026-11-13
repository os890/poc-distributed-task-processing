/*
 * Copyright 2026 os890
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.os890.dtp.core.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** Layering of the engine (docs/design/01-architecture.md): framework-free, domain without infrastructure. */
@AnalyzeClasses(packages = "org.os890.dtp", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule CORE_IS_FRAMEWORK_FREE = noClasses().that().resideInAPackage("org.os890.dtp..")
            .should().dependOnClassesThat().resideInAnyPackage("jakarta..", "io.quarkus..", "org.jboss..");

    @ArchTest
    static final ArchRule API_KNOWS_NO_IMPLEMENTATION = noClasses().that().resideInAPackage("org.os890.dtp.api..")
            .should().dependOnClassesThat().resideInAPackage("org.os890.dtp.core..");

    @ArchTest
    static final ArchRule DOMAIN_HAS_NO_INFRASTRUCTURE = noClasses().that().resideInAPackage("..core.domain..")
            .should().dependOnClassesThat().resideInAnyPackage("..core.engine..", "..core.jdbc..", "..core.query..",
                    "java.sql..", "javax.sql..", "com.fasterxml..", "org.flywaydb..");

    @ArchTest
    static final ArchRule PERSISTENCE_DOESNT_KNOW_THE_ENGINE = noClasses().that().resideInAPackage("..core.jdbc..")
            .should().dependOnClassesThat().resideInAnyPackage("..core.engine..", "..core.query..");

    @ArchTest
    static final ArchRule SPI_IS_STABLE = noClasses().that().resideInAPackage("..core.spi..")
            .should().dependOnClassesThat().resideInAnyPackage("..core.engine..", "..core.jdbc..");

    private ArchitectureTest() {
    }
}
