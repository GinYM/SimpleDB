package edu.berkeley.cs186.database.optimization;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.Map;

import edu.berkeley.cs186.database.table.stats.Histogram;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.Database;

import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import static org.junit.Assert.*;

public class TestInterestingOrders {
  private Table table;
  private Schema schema;
  public static final String TABLENAME = "T";

  public static final String TestDir = "testDatabase";
  private Database db;
  private String filename;


  //Before every test you create a temporary table, after every test you close it
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void beforeEach() throws Exception {
    
    File testDir = tempFolder.newFolder(TestDir);
    this.filename = testDir.getAbsolutePath();
    this.db = new Database(filename);
    this.db.deleteAllTables();

    this.schema = TestUtils.createSchemaWithAllTypes();

    db.createTable(this.schema, TABLENAME);

    db.createTableWithIndices(this.schema, TABLENAME+"I", Arrays.asList("int", "bool", "string", "float"));

    db.createTable(TestUtils.createSchemaWithAllTypes("one_"), TABLENAME+"o1");
    db.createTable(TestUtils.createSchemaWithAllTypes("two_"), TABLENAME+"o2");
    db.createTable(TestUtils.createSchemaWithAllTypes("three_"), TABLENAME+"o3");
    db.createTable(TestUtils.createSchemaWithAllTypes("four_"), TABLENAME+"o4");
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  //creates a record with all specified types
  private static Record createRecordWithAllTypes(boolean a1, int a2, String a3, float a4) {
    Record r = TestUtils.createRecordWithAllTypes();
    r.getValues().set(0, new BoolDataBox(a1));
    r.getValues().set(1, new IntDataBox(a2));
    r.getValues().set(2, new StringDataBox(a3,5));
    r.getValues().set(3, new FloatDataBox(a4));
    return r;
  }

  @Test
  public void testNoInterestingOrders() throws DatabaseException, QueryPlanException{
    Table table = db.getTable(TABLENAME);

    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    table.buildStatistics(10);

    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs(TABLENAME, "t1");

    QueryPlan query = transaction.query("t1");

    List<String> tableNames = new ArrayList<String>();
    tableNames.add("t1");
    Map<String, QueryOperator> pass1Map = new HashMap<String, QueryOperator>();

    for (String tbl : tableNames) {
      QueryOperator minOp = query.minCostSingleAccess(tbl);
      pass1Map.put(tbl, minOp);
    }

    Map<String, List<String>> interestingOrders = query.findInterestingOrders(pass1Map);

    assert(interestingOrders.isEmpty());
  }

  @Test
  public void testSingleInterestingOrder() throws DatabaseException, QueryPlanException{
    Table table = db.getTable(TABLENAME + "I");

    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    table.buildStatistics(10);

    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs(TABLENAME + "I", "t1");

    QueryPlan query = transaction.query("t1");
    query.groupBy("t1.bool");

    List<String> tableNames = new ArrayList<String>();
    tableNames.add("t1");
    Map<String, QueryOperator> pass1Map = new HashMap<String, QueryOperator>();

    for (String tbl : tableNames) {
      QueryOperator minOp = query.minCostSingleAccess(tbl);
      pass1Map.put(tbl, minOp);
    }

    Map<String, List<String>> interestingOrders = query.findInterestingOrders(pass1Map);

    assertFalse(interestingOrders.isEmpty());
    assert(interestingOrders.containsKey("t1"));
    assertFalse(interestingOrders.get("t1").isEmpty());
    assert(interestingOrders.get("t1").get(0).equals("t1.bool"));
  }

  @Test
  public void testMultipleInterestingOrders() throws DatabaseException, QueryPlanException{
    Table table = db.getTable(TABLENAME + "I");

    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    table.buildStatistics(10);

    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs(TABLENAME + "I", "t1");
    transaction.queryAs(TABLENAME, "t2");

    QueryPlan query = transaction.query("t1");
    query.groupBy("t1.bool");
    query.join("t2", "t1.int", "t2.int");

    List<String> tableNames = new ArrayList<String>();
    tableNames.add("t1");
    tableNames.add("t2");
    Map<String, QueryOperator> pass1Map = new HashMap<String, QueryOperator>();

    for (String tbl : tableNames) {
      QueryOperator minOp = query.minCostSingleAccess(tbl);
      pass1Map.put(tbl, minOp);
    }

    Map<String, List<String>> interestingOrders = query.findInterestingOrders(pass1Map);

    assertFalse(interestingOrders.isEmpty());
    assert(interestingOrders.containsKey("t1"));
    assertEquals(2, interestingOrders.get("t1").size());
  }

  @Test
  public void testMultipleTablesWithInterestingOrders() throws DatabaseException, QueryPlanException{
    Table table = db.getTable(TABLENAME + "I");

    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    table.buildStatistics(10);

    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs(TABLENAME + "I", "t1");
    transaction.queryAs(TABLENAME + "I", "t2");

    QueryPlan query = transaction.query("t1");
    query.groupBy("t1.bool");
    query.join("t2", "t1.int", "t2.int");

    List<String> tableNames = new ArrayList<String>();
    tableNames.add("t1");
    tableNames.add("t2");
    Map<String, QueryOperator> pass1Map = new HashMap<String, QueryOperator>();

    for (String tbl : tableNames) {
      QueryOperator minOp = query.minCostSingleAccess(tbl);
      pass1Map.put(tbl, minOp);
    }

    Map<String, List<String>> interestingOrders = query.findInterestingOrders(pass1Map);

    assertFalse(interestingOrders.isEmpty());
    assert(interestingOrders.containsKey("t1"));
    assert(interestingOrders.containsKey("t2"));
    assertEquals(2, interestingOrders.get("t1").size());
    assertEquals(1, interestingOrders.get("t2").size());
  }

  @Test
  public void testNotIncludeOptimal() throws DatabaseException, QueryPlanException{
    Table table = db.getTable(TABLENAME + "I");

    try{
      for (int i = 0; i < 1000; ++i) {
        Record r = createRecordWithAllTypes(false, i, "test", 0.0f);
        table.addRecord(r.getValues());
      }
    }
    catch(DatabaseException e){}

    table.buildStatistics(10);

    Database.Transaction transaction = this.db.beginTransaction();

    transaction.queryAs(TABLENAME + "I", "t1");
    transaction.queryAs(TABLENAME, "t2");

    QueryPlan query = transaction.query("t1");
    query.groupBy("t1.int");
    query.join("t2", "t1.int", "t2.int");
    query.select("t1.int", PredicateOperator.EQUALS, new IntDataBox(9));

    List<String> tableNames = new ArrayList<String>();
    tableNames.add("t1");
    tableNames.add("t2");
    Map<String, QueryOperator> pass1Map = new HashMap<String, QueryOperator>();

    for (String tbl : tableNames) {
      QueryOperator minOp = query.minCostSingleAccess(tbl);
      pass1Map.put(tbl, minOp);
    }

    Map<String, List<String>> interestingOrders = query.findInterestingOrders(pass1Map);

    assertTrue(interestingOrders.isEmpty());
  }
}