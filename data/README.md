# Dataset

All dataset files are attached to the GitHub Release `v1.0.0`:
https://github.com/clarkkiee/sre-core-orchestrator/releases/tag/v1.0.0

Each file is a gzip-compressed tar archive containing one CSV file.
Extract with: `tar -xzf <file>.csv.tar.gz`
SHA-256 checksums: `SHA256SUMS.txt` (attached to the same release).

| File                              | Description                                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| chaos_campaigns.csv.tar.gz        | Data on the execution of a series of failure injections in a single experimental cycle (chaos campaigns)                                                     |
| chaos_experiments.csv.tar.gz      | Data from chaos experiments or failure injection tests that have been conducted                                                                              |
| experiment_evaluations.csv.tar.gz | Data recording the results of reliability evaluations linking chaos experiments to related measurement indicators                                            |
| evaluation_indicators.csv.tar.gz  | Results of the measurement indicator (SLI) calculation                                                                                                       |
| probe_results.csv.tar.gz          | Data collected from LitmusProbe during the experiment                                                                                                        |
| clusters.csv.tar.gz               | Test environment cluster data generated during data collection                                                                                               |
| deployments.csv.tar.gz            | Data logging results from the deployment of microservices applications to a test environment cluster                                                         |
| jobs.csv.tar.gz                   | Experiment activity log during experiment runs                                                                                                               |
| raw_metric_samples.csv.tar.gz     | Data obtained by scraping time-series metrics from the microservices applications being tested, which will be used to calculate the specified SLI indicators |
