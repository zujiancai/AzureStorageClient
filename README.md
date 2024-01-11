# BatchJob

This project is to create a minimal batch job infrastructure to handle small data (fits in memory as a panda dataframe) on a single node. Different job type would be run in a separate process, and treated as singleton (no concurrent processing for the same job type).

## Setup

Install the package with command:

```bash
$ pip install "git+https://github.com/zujiancai/minimal-batch-job.git@main#egg=minimal-batch-job&subdirectory=src"
```

See under sample folder for usage cases.

## Design Details

### Scheduler and Runner

- `BaseJob` is the base class for batch job logic. All job type should extend this class. The entry point is the `run` method which takes a `JobInfo` object and returns a `JobRun` object as result. These methods are required to implement in solid subclasses: `create_items` to populate a list of items to loop through, for the first run generally; `process_one` to handle one item in the list; `post_loop` is optional for actions after all items being handled, for example saving result as a single file.

- `JobSettings` define the rules about job scheduling: when to trigger a new job, what is the maximum allowed failures, and the maximum consecutive failures before setting a job as failed, after how many hours to expire a incomplete job, what is the batch size for a single run, the processing interval between each item (throttling), the job identification (job type + version), and the `BaseJob` subclass type for creating a new job.

- `JobRunner` take the friendly job name as input so that different job type will run in its own process. It will resolve the friendly job name to `JobSettings`. For the given job settings, check if any existing active, pending, or suspended job to resume, to fail or to expire. If no existing to resume, check the job schedule to see if a new job should be created. If so, create and run the new job. If a job is executed and returns, store the `JobRun` object. `JobRunner` is supposed to run periodically, e.g. every 10 minutes, so that a job could be triggered and completed in a timely manner.

[FlowChart for JobRunner]

### Job Status

- Pending: job is just created without checking dependencies, or dependencies are not ready yet (need to wait for dependencies).

- Active: job is running or ready to run (dependency check is passed).

- Suspended: job is paused due to batch size constraint or exception/error.

- Completed: an end state. Job is finished successfully.

- Failed: an end state. Job is failed due to too many errors or consecutive errors.

- Expired: an end state. Job has not finished before the maximum hours for it to run. Newer inputs may be available for a job at a more recent time.

[Status Transition Chart]

### Data Schemas

#### JobInfo for job inputs and intermediate results.

- Table name: JobInfo

- Schema Version 1:

```python
class JobInfo(TypedDict):
    PartitionKey: str  # jobType_offsetVersion, e.g. LoadList_1001
    RowKey: str        # runDate_offsetRevision_PartitionKey
    revision: int      # default 0, increment it to rerun a job with same inputs.
    inputs: str        # dictionary in JSON
    states: str        # dictionary in JSON
    status: str        # current job status
    run_date: str      # in yyyyMMdd, e.g. 20231110
    create_time: datetime
    update_time: datetime
```

#### JobRun for job iteration history with end status.

- Table name: JobRun

- Schema Version 1:

```python
class JobRun(TypedDict):
    PartitionKey: str   # Reference to JobInfo RowKey
    RowKey: str         # endTime_PartitionKey
    is_error: bool
    message: str
    end_status: str     # the ending job status after this run
    start_time: datetime
    end_time: datetime
```

### Test Coverage with Azurite Emulator Enabled

| Name                      |Stmts  |Miss |Cover|
|---------------------------|-------|-----|-----|
|batch_job\__init__.py      |     4 |    0| 100%|
|batch_job\base_job.py      |    91 |    4|  96%|
|batch_job\blob_store.py    |    48 |   10|  79%|
|batch_job\job_data.py      |    94 |   18|  81%|
|batch_job\job_runner.py    |    46 |    5|  89%|
|batch_job\job_schedule.py  |    53 |    1|  98%|
|batch_job\job_settings.py  |    59 |    7|  88%|
|batch_job\table_store.py   |    36 |    0| 100%|
|tests\__init__.py          |     0 |    0| 100%|
|tests\mock_data.py         |    41 |    8|  80%|
|tests\test_base_job.py     |    75 |    0| 100%|
|tests\test_blob_store.py   |    66 |    2|  97%|
|tests\test_job_runner.py   |   178 |    1|  99%|
|tests\test_job_schedule.py |    54 |    0| 100%|
|tests\test_table_store.py  |    73 |    0| 100%|
|TOTAL                      |   918 |   56|  94%|