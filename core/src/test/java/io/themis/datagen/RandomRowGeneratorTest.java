package io.themis.datagen;

import io.themis.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class RandomRowGeneratorTest {

    @Test
    public void testGenerate() {
        for (Table table : SimpleTables.TABLES.values()) {
            RandomRowGenerator gen = new RandomRowGenerator(table, new RandomDataGenerator(), 10);
            int count = 0;
            for (Object[] row : gen) {
                Assertions.assertThat(row).hasSize(table.columns().size());
                count++;
            }
            Assertions.assertThat(count).isEqualTo(10);
        }
    }
}
