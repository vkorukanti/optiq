package org.apache.calcite.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Compatible;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * CachingCalciteSchema maintains a set of Navigable maps for each entity type.
 */
public class CachingCalciteSchema extends CalciteSchema{
  /** Comparator that compares all strings differently, but if two strings are
   * equal in case-insensitive match they are right next to each other. In a
   * collection sorted on this comparator, we can find case-insensitive matches
   * for a given string using a range scan between the upper-case string and
   * the lower-case string. */
  private static final Comparator<String> COMPARATOR =
      new Comparator<String>() {
        public int compare(String o1, String o2) {
          int c = o1.compareToIgnoreCase(o2);
          if (c == 0) {
            c = o1.compareTo(o2);
          }
          return c;
        }
      };
  /** Tables explicitly defined in this schema. Does not include tables in
   *  {@link #schema}. */
  public final NavigableMap<String, TableEntry> tableMap =
      new TreeMap<String, TableEntry>(COMPARATOR);
  private final Multimap<String, FunctionEntry> functionMap =
      LinkedListMultimap.create();
  private final NavigableMap<String, LatticeEntry> latticeMap =
      new TreeMap<String, LatticeEntry>(COMPARATOR);
  private final NavigableSet<String> functionNames =
      new TreeSet<String>(COMPARATOR);
  private final NavigableMap<String, FunctionEntry> nullaryFunctionMap =
      new TreeMap<String, FunctionEntry>(COMPARATOR);
  private final NavigableMap<String, CalciteSchema> subSchemaMap =
      new TreeMap<String, CalciteSchema>(COMPARATOR);

  private final Cached<SubSchemaCache> implicitSubSchemaCache;
  private final Cached<NavigableSet<String>> implicitTableCache;
  private final Cached<NavigableSet<String>> implicitFunctionCache;

  public CachingCalciteSchema(CalciteSchema parent, final Schema schema,
                            String name) {
    super(parent, schema, name);
    assert (parent == null) == (this instanceof CalciteRootSchema);
    this.implicitSubSchemaCache =
        new AbstractCached<SubSchemaCache>() {
          public SubSchemaCache build() {
            return new SubSchemaCache(CalciteSchema.this,
                Compatible.INSTANCE.navigableSet(
                    ImmutableSortedSet.copyOf(COMPARATOR,
                        schema.getSubSchemaNames())));
          }
        };
    this.implicitTableCache =
        new AbstractCached<NavigableSet<String>>() {
          public NavigableSet<String> build() {
            return Compatible.INSTANCE.navigableSet(
                ImmutableSortedSet.copyOf(COMPARATOR,
                    schema.getTableNames()));
          }
        };
    this.implicitFunctionCache =
        new AbstractCached<NavigableSet<String>>() {
          public NavigableSet<String> build() {
            return Compatible.INSTANCE.navigableSet(
                ImmutableSortedSet.copyOf(COMPARATOR,
                    schema.getFunctionNames()));
          }
        };

  }

  /** Defines a table within this schema. */
  public TableEntry add(String tableName, Table table,
                                 ImmutableList<String> sqls) {
    final TableEntryImpl entry =
        new TableEntryImpl(this, tableName, table, sqls);
    tableMap.put(tableName, entry);
    return entry;
  }

  /** Strategy for caching the value of an object and re-creating it if its
   * value is out of date as of a given timestamp.
   *
   * @param <T> Type of cached object
   */
  private interface Cached<T> {
    /** Returns the value; uses cached value if valid. */
    T get(long now);

    /** Creates a new value. */
    T build();

    /** Called when CalciteSchema caching is enabled or disabled. */
    void enable(long now, boolean enabled);
  }

  /** Implementation of {@link CalciteSchema.Cached}
   * that drives from {@link CalciteSchema#cache}. */
  private abstract class AbstractCached<T> implements Cached<T> {
    T t;
    long checked = Long.MIN_VALUE;

    public T get(long now) {
      if (!CachingCalciteSchema.this.cache) {
        return build();
      }
      if (checked == Long.MIN_VALUE
          || schema.contentsHaveChangedSince(checked, now)) {
        t = build();
      }
      checked = now;
      return t;
    }

    public void enable(long now, boolean enabled) {
      if (!enabled) {
        t = null;
      }
      checked = Long.MIN_VALUE;
    }
  }

  /** Information about the implicit sub-schemas of an {@link CalciteSchema}. */
  private static class SubSchemaCache {
    /** The names of sub-schemas returned from the {@link Schema} SPI. */
    final NavigableSet<String> names;
    /** Cached {@link CalciteSchema} wrappers. It is
     * worth caching them because they contain maps of their own sub-objects. */
    final LoadingCache<String, CalciteSchema> cache;

    private SubSchemaCache(final CalciteSchema calciteSchema,
                           NavigableSet<String> names) {
      this.names = names;
      this.cache = CacheBuilder.newBuilder().build(
          new CacheLoader<String, CalciteSchema>() {
            @SuppressWarnings("NullableProblems")
            @Override public CalciteSchema load(String schemaName) {
              final Schema subSchema =
                  calciteSchema.schema.getSubSchema(schemaName);
              if (subSchema == null) {
                throw new RuntimeException("sub-schema " + schemaName
                    + " not found");
              }
              return new CachingCalciteSchema(calciteSchema, subSchema, schemaName);
            }
          });
    }
  }
}
