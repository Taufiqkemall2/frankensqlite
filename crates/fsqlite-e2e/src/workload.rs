//! Deterministic workload generation with seeded RNG.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A deterministic workload generator backed by a seeded PRNG.
///
/// Given the same seed, it always produces the identical sequence of SQL
/// statements, enabling reproducible differential testing.
#[derive(Debug)]
pub struct WorkloadGenerator {
    rng: StdRng,
    /// Maximum number of tables to create.
    pub max_tables: usize,
    /// Maximum number of rows to insert per table.
    pub max_rows_per_table: usize,
}

impl WorkloadGenerator {
    /// Create a new generator from a fixed seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            max_tables: 4,
            max_rows_per_table: 100,
        }
    }

    /// Generate a batch of SQL statements that create tables and populate them.
    pub fn generate_insert_workload(&mut self) -> Vec<String> {
        let num_tables = self.rng.gen_range(1..=self.max_tables);
        let mut stmts = Vec::new();

        for t in 0..num_tables {
            let table_name = format!("t{t}");
            stmts.push(format!(
                "CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, val TEXT, num REAL)"
            ));

            let num_rows = self.rng.gen_range(1..=self.max_rows_per_table);
            for r in 0..num_rows {
                let val_len = self.rng.gen_range(1..=32);
                let val: String = (0..val_len)
                    .map(|_| {
                        let idx = self.rng.gen_range(0..26);
                        (b'a' + idx) as char
                    })
                    .collect();
                let num: f64 = self.rng.gen_range(-1000.0..1000.0);
                stmts.push(format!(
                    "INSERT INTO {table_name} (id, val, num) VALUES ({r}, '{val}', {num})"
                ));
            }
        }

        stmts
    }

    /// Generate a batch of SELECT queries targeting tables created by
    /// `generate_insert_workload`.
    pub fn generate_query_workload(&mut self, table_count: usize) -> Vec<String> {
        let mut stmts = Vec::new();
        for t in 0..table_count {
            let table_name = format!("t{t}");
            stmts.push(format!("SELECT COUNT(*) FROM {table_name}"));
            stmts.push(format!("SELECT * FROM {table_name} ORDER BY id"));
            stmts.push(format!(
                "SELECT val, num FROM {table_name} WHERE num > 0 ORDER BY num"
            ));
        }
        stmts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_workload() {
        let stmts_a = WorkloadGenerator::new(42).generate_insert_workload();
        let stmts_b = WorkloadGenerator::new(42).generate_insert_workload();
        assert_eq!(
            stmts_a, stmts_b,
            "same seed must produce identical workload"
        );
    }

    #[test]
    fn test_different_seeds_differ() {
        let stmts_a = WorkloadGenerator::new(1).generate_insert_workload();
        let stmts_b = WorkloadGenerator::new(2).generate_insert_workload();
        assert_ne!(stmts_a, stmts_b, "different seeds should differ");
    }
}
