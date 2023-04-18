package io.themis.tpcds;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import io.themis.Table;
import io.themis.TableDataGenerator;

import java.util.Iterator;

class TpcdsStringRowGeneratorTest {

    @Test
    public void test1GBP1000Child1RowCountMatches() {
        for (Table table : TpcdsTables.SF1_P1000_ID1_ROW_COUNTS.keySet()) {
            TableDataGenerator<String[]> gen = new TpcdsStringRowGenerator(
                    table, 1, 1000, 1);
            int count = 0;
            for (String[] line : gen) {
                Assertions.assertThat(table.columns()).hasSize(line.length);
                count++;
            }
            Assertions.assertThat(TpcdsTables.SF1_P1000_ID1_ROW_COUNTS.get(table))
                    .isEqualTo(count);
        }
    }

    @Test
    public void test1GBP1000Child2RowCountMatches() {
        for (Table table : TpcdsTables.SF1_P1000_ID2_ROW_COUNTS.keySet()) {
            TableDataGenerator<String[]> gen = new TpcdsStringRowGenerator(
                    table, 1, 1000, 2);
            int count = 0;
            for (String[] line : gen) {
                Assertions.assertThat(table.columns()).hasSize(line.length);
                count++;
            }
            Assertions.assertThat(TpcdsTables.SF1_P1000_ID2_ROW_COUNTS.get(table))
                    .isEqualTo(count);
        }
    }

    @Test
    public void testCallCenterDataMatches() {
        TableDataGenerator<String[]> gen = new TpcdsStringRowGenerator(
                TpcdsTables.CALL_CENTER, 1, 1, 1);
        Iterator<String[]> iterator = gen.iterator();
        for (String[] row : TpcdsTables.CALL_CENTER_DATA) {
            Assertions.assertThat(iterator.hasNext()).isTrue();
            Assertions.assertThat(iterator.next()).containsExactly(row);
        }
    }
}