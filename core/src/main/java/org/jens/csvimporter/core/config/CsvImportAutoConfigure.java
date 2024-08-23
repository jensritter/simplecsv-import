package org.jens.csvimporter.core.config;

import org.jens.csvimporter.core.CsvImporter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
@Configuration
@ComponentScan(basePackageClasses=CsvImporter.class)
public class CsvImportAutoConfigure {


}
