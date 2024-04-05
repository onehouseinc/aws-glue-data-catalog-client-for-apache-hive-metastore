package com.amazonaws.glue.catalog.converters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.common.util.HiveVersionInfo;

public class CatalogToHiveConverterFactory {

  private static final String HIVE_3_VERSION = "3.";

  private static CatalogToHiveConverter catalogToHiveConverter;

  public static CatalogToHiveConverter getCatalogToHiveConverter() throws MetaException {
    if (catalogToHiveConverter == null) {
      catalogToHiveConverter = loadConverter();
    }
    return catalogToHiveConverter;
  }

  private static CatalogToHiveConverter loadConverter() throws MetaException {
    String hiveVersion = HiveVersionInfo.getShortVersion();

    if (hiveVersion.startsWith(HIVE_3_VERSION)) {
      throw new MetaException("Hive3 is not supported");
    } else {
      return new BaseCatalogToHiveConverter();
    }
  }

  @VisibleForTesting
  static void clearConverter() {
    catalogToHiveConverter = null;
  }
}
