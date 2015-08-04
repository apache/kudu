// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.WireProtocol.RowOperationsPB;
import org.kududb.client.Operation.ChangeType;
import org.kududb.tserver.Tserver.WriteRequestPBOrBuilder;
import org.mockito.Mockito;

import com.google.common.primitives.Longs;

/**
 * Unit tests for Operation
 */
public class TestOperation {

  private Schema createManyStringsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING).nullable(true).build());
    return new Schema(columns);
  }

  @Test
  public void testSetStrings() {
    KuduTable table = Mockito.mock(KuduTable.class);
    Mockito.doReturn(createManyStringsSchema()).when(table).getSchema();
    Insert insert = new Insert(table);
    PartialRow row = insert.getRow();
    row.addString("c0", "c0_val");
    row.addString("c2", "c2_val");
    row.addString("c1", "c1_val");
    row.addString("c3", "c3_val");
    row.addString("c4", "c4_val");

    {
      WriteRequestPBOrBuilder pb = Operation.createAndFillWriteRequestPB(insert);
      RowOperationsPB rowOps = pb.getRowOperations();
      assertEquals(6 * 5, rowOps.getIndirectData().size());
      assertEquals("c0_valc1_valc2_valc3_valc4_val", rowOps.getIndirectData().toStringUtf8());
      byte[] rows = rowOps.getRows().toByteArray();
      assertEquals(ChangeType.INSERT.toEncodedByte(), rows[0]);
      // The "isset" bitset should have 5 bits set
      assertEquals(0x1f, rows[1]);
      // The "null" bitset should have no bits set
      assertEquals(0, rows[2]);

      // Check the strings.
      int offset = 3;
      for (int i = 0; i <= 4; i++) {
        // The offset into the indirect buffer
        assertEquals(6 * i, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
        // The length of the pointed-to string.
        assertEquals(6, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
      }

      // Should have used up whole buffer.
      assertEquals(rows.length, offset);
    }

    // Setting a field to NULL should add to the null bitmap and remove
    // the old value from the indirect buffer.
    row.setNull("c3");
    {
      WriteRequestPBOrBuilder pb = Operation.createAndFillWriteRequestPB(insert);
      RowOperationsPB rowOps = pb.getRowOperations();
      assertEquals(6 * 4, rowOps.getIndirectData().size());
      assertEquals("c0_valc1_valc2_valc4_val", rowOps.getIndirectData().toStringUtf8());
      byte[] rows = rowOps.getRows().toByteArray();
      assertEquals(ChangeType.INSERT.toEncodedByte(), rows[0]);
      // The "isset" bitset should have 5 bits set
      assertEquals(0x1f, rows[1]);
      // The "null" bitset should have 1 bit set for the null column
      assertEquals(1 << 3, rows[2]);

      // Check the strings.
      int offset = 3;
      int indirOffset = 0;
      for (int i = 0; i <= 4; i++) {
        if (i == 3) continue;
        // The offset into the indirect buffer
        assertEquals(indirOffset, Bytes.getLong(rows, offset));
        indirOffset += 6;
        offset += Longs.BYTES;
        // The length of the pointed-to string.
        assertEquals(6, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
      }
      // Should have used up whole buffer.
      assertEquals(rows.length, offset);
    }
  }

}
