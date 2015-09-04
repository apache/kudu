// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

import static org.kududb.master.Master.AlterTableRequestPB;

/**
 * This builder must be used to alter a table. At least one change must be specified.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class AlterTableBuilder {

  AlterTableRequestPB.Builder pb = AlterTableRequestPB.newBuilder();

  /**
   * Change a table's name.
   * @param newName New table's name, must be used to check progress
   */
  public void renameTable(String newName) {
    pb.setNewTableName(newName);
  }

  /**
   * Add a new column that's not nullable
   * @param name Name of the new column
   * @param type Type of the new column
   * @param defaultVal Default value used for the currently existing rows
   */
  public void addColumn(String name, Type type, Object defaultVal) {
    if (defaultVal == null) {
      throw new IllegalArgumentException("A new column must have a default value, " +
          "use addNullableColumn() to add a NULLABLE column");
    }
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ADD_COLUMN);
    step.setAddColumn(AlterTableRequestPB.AddColumn.newBuilder().setSchema(ProtobufHelper
        .columnToPb(new ColumnSchema.ColumnSchemaBuilder(name, type)
            .defaultValue(defaultVal)
            .build())));
  }

  /**
   * Add a new column that's nullable, thus has no default value
   * @param name Name of the new column
   * @param type Type of the new column
   */
  public void addNullableColumn(String name, Type type) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ADD_COLUMN);
    step.setAddColumn(AlterTableRequestPB.AddColumn.newBuilder().setSchema(ProtobufHelper
        .columnToPb(new ColumnSchema.ColumnSchemaBuilder(name, type)
            .nullable(true)
            .build())));
  }

  /**
   * Drop a column
   * @param name Name of the column
   */
  public void dropColumn(String name) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.DROP_COLUMN);
    step.setDropColumn(AlterTableRequestPB.DropColumn.newBuilder().setName(name));
  }

  /**
   * Change the name of a column
   * @param oldName Old column's name, must exist
   * @param newName New name to use
   */
  public void renameColumn(String oldName, String newName) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.RENAME_COLUMN);
    step.setRenameColumn(AlterTableRequestPB.RenameColumn.newBuilder().setOldName(oldName)
        .setNewName(newName));
  }
}
