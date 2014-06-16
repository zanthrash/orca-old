create table batch_job_instance  (
	job_instance_id bigint primary key ,
	version bigint,
	job_name varchar,
	job_key varchar,
) ;

create index on batch_job_instance (job_name);
create index on batch_job_instance (job_key);

create table batch_job_execution  (
	job_execution_id bigint,
	version bigint  ,
	job_instance_id bigint,
	create_time timestamp,
	start_time timestamp,
	ended boolean,
	end_time timestamp,
	status varchar ,
	exit_code varchar ,
	exit_message varchar ,
	last_updated timestamp,
	job_configuration_location varchar,
	primary key ((job_execution_id), job_instance_id)
) ;

create index on batch_job_execution (ended);

create table batch_job_execution_params  (
	job_execution_id bigint,
	type_cd varchar,
	key_name varchar,
	string_val varchar,
	date_val timestamp,
	long_val bigint ,
	double_val double,
	identifying varchar
) ;

create table batch_step_execution  (
	step_execution_id bigint primary key ,
	version bigint,
	step_name varchar,
	job_execution_id bigint,
	start_time timestamp,
	end_time timestamp ,
	status varchar ,
	commit_count bigint ,
	read_count bigint ,
	filter_count bigint ,
	write_count bigint ,
	read_skip_count bigint ,
	write_skip_count bigint ,
	process_skip_count bigint ,
	rollback_count bigint ,
	exit_code varchar,
	exit_message varchar,
	last_updated timestamp
) ;

create table batch_step_execution_context  (
	step_execution_id bigint primary key,
	short_context varchar,
	serialized_context text
) ;

create table batch_job_execution_context  (
	job_execution_id bigint primary key,
	short_context varchar,
	serialized_context text
) ;
