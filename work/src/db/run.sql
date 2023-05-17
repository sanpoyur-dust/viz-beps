CREATE VIEW k562_rep1 AS
SELECT
k562.id,
k562.sample,
k562.guideid,
k562.aligned_sequence,
k562.read_status,
k562.reference_sequence,
k562.deletion_positions,
k562.deletion_sizes,
k562.insertion_positions,
k562.insertion_sizes,
k562.substitution_positions,
k562.substitution_values,
k562.n_deleted,
k562.n_inserted,
k562.n_mutated,
k562.reads
FROM k562
WHERE k562.rep = 'Rep1';

CREATE VIEW k562_rep2 AS
SELECT
k562.id,
k562.sample,
k562.guideid,
k562.aligned_sequence,
k562.read_status,
k562.reference_sequence,
k562.deletion_positions,
k562.deletion_sizes,
k562.insertion_positions,
k562.insertion_sizes,
k562.substitution_positions,
k562.substitution_values,
k562.n_deleted,
k562.n_inserted,
k562.n_mutated,
k562.reads
FROM k562
WHERE k562.rep = 'Rep2';

-- full joining on rep-agnostic columns
CREATE MATERIALIZED VIEW k562_combined AS
SELECT
COALESCE(k562_rep1.id, k562_rep2.id) AS id,
COALESCE(k562_rep1.reads, 0) AS rep1,
COALESCE(k562_rep2.reads, 0) AS rep2
FROM k562_rep1
FULL JOIN k562_rep2
USING (
  sample,
  guideid,
  aligned_sequence,
  read_status,
  reference_sequence,
  deletion_positions,
  deletion_sizes,
  insertion_positions,
  insertion_sizes,
  substitution_positions,
  substitution_values,
  n_deleted,
  n_inserted,
  n_mutated
);

CREATE VIEW k562_combined_test AS
SELECT
k562_combined.id,
k562_combined.rep1,
k562_combined.rep2,
k562.sample,
k562.guideid,
k562.aligned_sequence,
k562.read_status,
k562.reference_sequence,
k562.deletion_positions,
k562.deletion_sizes,
k562.insertion_positions,
k562.insertion_sizes,
k562.substitution_positions,
k562.substitution_values,
k562.n_deleted,
k562.n_inserted,
k562.n_mutated
FROM k562_combined
INNER JOIN k562
USING (id)
WHERE k562.sample != 'Samp0';

CREATE VIEW k562_combined_ctrl AS
SELECT
k562_combined.id,
k562_combined.rep1,
k562_combined.rep2,
k562.sample,
k562.guideid,
k562.aligned_sequence,
k562.read_status,
k562.reference_sequence,
k562.deletion_positions,
k562.deletion_sizes,
k562.insertion_positions,
k562.insertion_sizes,
k562.substitution_positions,
k562.substitution_values,
k562.n_deleted,
k562.n_inserted,
k562.n_mutated
FROM k562_combined
INNER JOIN k562
USING (id)
WHERE k562.sample = 'Samp0';

-- full join on sample-agnostic columns
CREATE MATERIALIZED VIEW k562_compare AS
SELECT
COALESCE(k562_combined_test.id, 0) AS id,
COALESCE(k562_combined_test.rep1, 0) AS rep1,
COALESCE(k562_combined_test.rep2, 0) AS rep2,
COALESCE(k562_combined_ctrl.id, 0) AS id_ctrl,
COALESCE(k562_combined_ctrl.rep1, 0) AS rep1_ctrl,
COALESCE(k562_combined_ctrl.rep2, 0) AS rep2_ctrl
FROM k562_combined_test
FULL JOIN k562_combined_ctrl
USING (
  guideid,
  aligned_sequence,
  read_status,
  reference_sequence,
  deletion_positions,
  deletion_sizes,
  insertion_positions,
  insertion_sizes,
  substitution_positions,
  substitution_values,
  n_deleted,
  n_inserted,
  n_mutated
);

CREATE MATERIALIZED VIEW k562_diff AS
SELECT
COALESCE(NULLIF(k562_compare.id, 0), k562_compare.id_ctrl) AS id,
((rep1 - rep1_ctrl) + (rep2 - rep2_ctrl)) / 2 AS propdiff
FROM k562_compare;

-- filter for significant differences
-- this also cleans away control samples
CREATE MATERIALIZED VIEW k562_diff_substs AS
SELECT
k562_diff.id,
k562_diff.propdiff,
k562.sample,
k562.guideid,
k562.substitution_positions,
k562.substitution_values
FROM k562_diff
INNER JOIN k562
USING (id)
WHERE k562_diff.propdiff >= 0.1;
