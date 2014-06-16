## Ideas

* It might be a good idea to de-normalize `batch_job_execution` to contain a column for the job name as we query by that frequently. It would also mean we wouldn't need a separate lookup to retrieve on `batch_job_instance` in order to build a `JobExecution` object.