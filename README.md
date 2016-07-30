# aws-swf-task-rerun
AWS Simple Work Flow (SWF) task re-run tool for timed-out tasks

The motivation to create this script is to allow SWF admin to re-run timed out tasks due to instances (code deployment) problem which causes tasks could not be processed properly and become "TIMED OUT" in SWF.

After chatting with AWS support, I found out that there's no simple way to re-run the tasks in batch. Hence, this script is created. My aim is to be able to re-run timed out tasks that are in large numbers (e.g. 9,000 tasks).

## How to Run?

To check the options of the script, run: python rerun-swf-timedout-tasks.py -h

Required inputs:
- AWS region
- SWF domain
- start date filter (where to look for timed out tasks)

This script will fetch tasks from the 'start date filter' to current date.

The reason it is not straight forward because:
- date time must be provided in epoch format (at least for aws-cli)
- JSON document must be composed to re-run a single task

When I got more time, I will make it run in parallel.

Enjoy!
