"""Take in CSV results from benchmark and aggregate to tables."""
import duckdb

with duckdb.connect(database="storage-benchmark.duckdb", read_only=False) as con:
    con.execute(
        """
    create or replace table main.file_counts as
    select
        phase,
        case format
            when 'delta' then 'Delta Lake'
            when 'iceberg' then 'Apache Iceberg'
        end as format,
        sum(files_added)
            over (partition by format order by phase_num) -
        sum(files_removed)
            over (partition by format order by phase_num)
        as total_files
    from 'csv/file_counts.csv'
    ;
        """
    )
    con.execute(
        """
    create or replace table main.file_diffs as
    with delta_files as (
        select
            phase,
            total_files as delta_total_files
        from main.file_counts
        where format = 'Delta Lake'
    ), iceberg_files as (
        select
            phase,
            total_files as iceberg_total_files
        from main.file_counts
        where format = 'Apache Iceberg'
    ), ratio as (
        select
            delta_files.phase,
            delta_files.delta_total_files,
            iceberg_files.iceberg_total_files,
            round(
                iceberg_files.iceberg_total_files /
                delta_files.delta_total_files,
                2
            ) as file_count_ratio,
        from delta_files
        inner join iceberg_files
            on delta_files.phase = iceberg_files.phase
    )
    select * from ratio
    ;
        """
    )

    con.executemany(
        """
    create or replace table main.stats as
    (
        select
            case format
                when 'delta' then 'Delta Lake'
                when 'iceberg' then 'Apache Iceberg'
            end as format,
            mutation as phase_num,
            case mutation
                when 1 then 'Create Table'
                when 2 then 'Upsert Medium Data'
                when 3 then 'Delete Extra Small Data'
                when 4 then 'Delete Small Data'
                when 5 then 'Delete Medium Data'
                when 6 then 'GDPR Request'
            end as phase_name,
            run_times_iteration,
            read_write as type,
            name,
            durationSec as duration_sec,
            log(durationSec) as log_duration_sec
        from 'csv/concat_results.csv'
    );

    create or replace table main.geomeans as
    (
        select
            format,
            type,
            phase_num,
            phase_name,
            round(
                pow(
                    10,
                    sum(log_duration_sec) / count(*)
                )
                , 0
            ) as geomean_duration_sec,
        from stats
    group by 1, 2, 3, 4
    );

    create or replace table main.read_results as
    with delta_reads as (
        select
            phase_num,
            phase_name,
            geomean_duration_sec as delta_read_geomean_sec
        from geomeans
        where
            type = 'read'
            and format = 'Delta Lake'
    ), iceberg_reads as (
        select
            phase_num,
            phase_name,
            geomean_duration_sec as iceberg_read_geomean_sec
        from geomeans
        where
            type = 'read'
            and format = 'Apache Iceberg'
    ), reads as (
        select
            delta_reads.phase_num,
            delta_reads.phase_name,
            delta_reads.delta_read_geomean_sec,
            iceberg_reads.iceberg_read_geomean_sec,
            round(
                iceberg_reads.iceberg_read_geomean_sec /
                delta_reads.delta_read_geomean_sec,
                2
            ) as read_ratio,
        from delta_reads
        inner join iceberg_reads
            on delta_reads.phase_num = iceberg_reads.phase_num
    )
    select * from reads
    ;

    create or replace table main.write_results as
    with delta_writes as (
        select
            phase_num,
            phase_name,
            geomean_duration_sec as delta_write_geomean_sec
        from geomeans
        where
            type = 'write'
            and format = 'Delta Lake'
    ), iceberg_writes as (
        select
            phase_num,
            phase_name,
            geomean_duration_sec as iceberg_write_geomean_sec
        from geomeans
        where
            type = 'write'
            and format = 'Apache Iceberg'
    ), writes as (
        select
            delta_writes.phase_num,
            delta_writes.phase_name,
            delta_writes.delta_write_geomean_sec,
            iceberg_writes.iceberg_write_geomean_sec,
            round(
                iceberg_writes.iceberg_write_geomean_sec /
                delta_writes.delta_write_geomean_sec,
                2
            ) as write_ratio,
        from delta_writes
        inner join iceberg_writes
            on delta_writes.phase_name = iceberg_writes.phase_name
    )
    select * from writes
    ;

    create or replace table main.total_results as
    with results as (
        select
            read_results.*,
            write_results.* exclude (phase_name),
        from read_results
        inner join write_results
            on read_results.phase_name = write_results.phase_name
    )
    select * from results
    ;
        """
    )
