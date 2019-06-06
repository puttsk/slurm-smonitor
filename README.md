# slurm-smonitor
Command line monitoring tools for HPC systems with Slurm workload manager

## Example Usage

```
python -m smonitor utilization --freq week 
python -m smonitor usage --groups_by account,user --groups_by_field elapsed_raw,su_usage  --format json
```
