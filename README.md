# slurm-smonitor
Command line monitoring tools for HPC systems with Slurm workload manager

## Example Usage

```
# Report weekly utilization. Print output to STDOUT as table.
python -m smonitor utilization --freq week 

# Report weekly SU usage grouped by account. Print JSON output to STDOUT.
python -m smonitor usage --groups_by account --freq week --groups_by_field su_usage  --format json

# Report total CPU time in seconds and SU usage. Print JSON output to STDOUT.
# Groups the result by users inside an account. 
python -m smonitor usage --groups_by account,user --groups_by_field elapsed_raw,su_usage  --format json
```
