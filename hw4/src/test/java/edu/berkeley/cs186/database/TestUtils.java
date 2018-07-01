package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {
  public static Schema createSchemaWithAllTypes() {
    List<String> names = Arrays.asList("bool", "int", "string", "float");
    List<Type> types = Arrays.asList(Type.boolType(), Type.intType(),
                                     Type.stringType(5), Type.floatType());
    return new Schema(names, types);
  }

  public static Schema createSchemaWithAllTypes(String prefix) {
    List<String> names = Arrays.asList(prefix+"bool", prefix+"int", prefix+"string", prefix+"float");
    List<Type> types = Arrays.asList(Type.boolType(), Type.intType(),
                                     Type.stringType(5), Type.floatType());
    return new Schema(names, types);
  }

  public static Schema createSchemaWithTwoInts() {
    List<Type> dataBoxes = new ArrayList<Type>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(Type.intType());
    dataBoxes.add(Type.intType());

    fieldNames.add("int1");
    fieldNames.add("int2");

    return new Schema(fieldNames, dataBoxes);
  }

  public static Schema createSchemaOfBool() {
    List<Type> dataBoxes = new ArrayList<Type>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(Type.boolType());

    fieldNames.add("bool");

    return new Schema(fieldNames, dataBoxes);
  }

  public static Schema createSchemaOfString(int len) {
    List<Type> dataBoxes = new ArrayList<Type>();
    List<String> fieldNames = new ArrayList<String>();

    dataBoxes.add(Type.stringType(len));
    fieldNames.add("string");

    return new Schema(fieldNames, dataBoxes);
  }


  public static Record createRecordWithAllTypes() {
    List<DataBox> dataValues = new ArrayList<DataBox>();
    dataValues.add(new BoolDataBox(true));
    dataValues.add(new IntDataBox(1));
    dataValues.add(new StringDataBox("abcde", 5));
    dataValues.add(new FloatDataBox((float) 1.2));

    return new Record(dataValues);
  }

  public static Record createRecordWithAllTypesWithValue(int val) {
    List<DataBox> dataValues = new ArrayList<DataBox>();
    dataValues.add(new BoolDataBox(true));
    dataValues.add(new IntDataBox(val));
    dataValues.add(new StringDataBox(String.format("%05d", val), 5));
    dataValues.add(new FloatDataBox((float) val));
    return new Record(dataValues);
  }

}
