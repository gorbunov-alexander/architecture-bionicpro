CREATE TABLE prosthesis_reports (
                                    user_id LowCardinality(String),
                                    date String,
                                    crm_name String,
                                    crm_age UInt8,
                                    crm_gender LowCardinality(String),

                                    prosthesis_type LowCardinality(String),
                                    muscle_group LowCardinality(String),

                                    signals_count UInt32,
                                    signal_frequency_avg Float32,
                                    signal_duration_avg Float32,
                                    signal_amplitude_avg Float32,
                                    signal_duration_total UInt32
) ENGINE = SummingMergeTree()
PARTITION BY left(date, 7)
ORDER BY (user_id, date, prosthesis_type, muscle_group)
SETTINGS index_granularity = 8192;