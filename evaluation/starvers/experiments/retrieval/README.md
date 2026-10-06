# Retrieval Experiment (placeholder)

Placeholder for the planned GraphDB vs. Jena TDB2 **retrieval** experiment that
empirically tests the theoretical cost analysis from the paper's "RDF stores"
section (sn-article.tex).

## Planned experiment

Three synthetic 1M-triple datasets (decorator / `tb_sr_rs` model) that cross the
two store-specific cost parameters:

| Dataset | Jena candidates `C` (valid_until=9999) | GraphDB matches `M` (?s rdf:type Film) | Expected |
|---------|----------------------------------------|---------------------------------------|----------|
| D1      | 10                                     | 10                                    | equal    |
| D2      | 1,000,000                              | 10                                    | Jena slower |
| D3      | 10                                     | 1,000,000                             | GraphDB slower |

- Query `Q`: `<< <<?s rdf:type Film>> vers:valid_from "2022-10-01" >> vers:valid_until "9999-12-31" .`
- Measures query runtime on GraphDB 10.5 and Jena TDB2 5.1 for each dataset.
