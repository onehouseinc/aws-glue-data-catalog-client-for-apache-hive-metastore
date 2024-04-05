package com.amazonaws.glue.catalog.converters;

import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest(HiveVersionInfo.class)
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class CatalogToHiveConverterFactoryTest {

  private static final String HIVE_1_2_VERSION = "1.2.1";

  @Before
  public void setup() throws ClassNotFoundException {
    mockStatic(HiveVersionInfo.class);
  }

  @After
  public void reset() {
    CatalogToHiveConverterFactory.clearConverter();
  }

  @Test
  public void testGetBaseCatalogToHiveConverter() throws Exception {
    when(HiveVersionInfo.getShortVersion()).thenReturn(HIVE_1_2_VERSION);
    CatalogToHiveConverter catalogToHiveConverter = CatalogToHiveConverterFactory.getCatalogToHiveConverter();
    assertTrue(BaseCatalogToHiveConverter.class.isInstance(catalogToHiveConverter));
  }
}
