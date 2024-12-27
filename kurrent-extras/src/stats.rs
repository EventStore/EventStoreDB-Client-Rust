use chrono::{DateTime, Utc};
use std::{collections::BTreeMap, num::IntErrorKind};

#[derive(Debug, Default, Clone)]
pub struct Statistics {
    pub proc: Proc,
    pub sys: Sys,
    pub es: Es,
}

pub trait StatisticsExt {
    fn parse_statistics(self) -> kurrent::Result<Statistics>;
}

impl StatisticsExt for kurrent::operations::RawStatistics {
    fn parse_statistics(self) -> kurrent::Result<Statistics> {
        let mut stats = Statistics::default();

        for (key, value) in self.0 {
            match key.as_str() {
                "proc-startTime" => {
                    stats.proc.start_time = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-id" => {
                    stats.proc.id = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-mem" => {
                    stats.proc.mem = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-cpu" => {
                    stats.proc.cpu = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-threadsCount" => {
                    stats.proc.threads_count = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-contentionsRate" => {
                    stats.proc.contentions_rate = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-thrownExceptionsRate" => {
                    stats.proc.thrown_exceptions_rate = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-allocationSpeed" => {
                    stats.proc.gc.allocation_speed = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen0ItemsCount" => {
                    stats.proc.gc.gen0_items_count = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen0Size" => {
                    stats.proc.gc.gen0_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen1ItemsCount" => {
                    stats.proc.gc.gen1_items_count = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen1Size" => {
                    stats.proc.gc.gen1_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen2ItemsCount" => {
                    stats.proc.gc.gen2_items_count = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-gen2Size" => {
                    stats.proc.gc.gen2_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-largeHeapSize" => {
                    stats.proc.gc.large_heap_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-timeInGc" => {
                    stats.proc.gc.time_in_gc = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-gc-totalBytesInHeaps" => {
                    stats.proc.gc.total_bytes_in_heaps = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-diskIo-readBytes" => {
                    stats.proc.disk_io.read_bytes = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-diskIo-writtenBytes" => {
                    stats.proc.disk_io.written_bytes = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-diskIo-readOps" => {
                    stats.proc.disk_io.read_ops = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-diskIo-writeOps" => {
                    stats.proc.disk_io.write_ops = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-connections" => {
                    stats.proc.tcp.connections = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-receivingSpeed" => {
                    stats.proc.tcp.receiving_speed = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-sendingSpeed" => {
                    stats.proc.tcp.sending_speed = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-inSend" => {
                    stats.proc.tcp.in_send = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-measureTime" => {
                    stats.proc.tcp.measure_time = value;
                }

                "proc-tcp-pendingReceived" => {
                    stats.proc.tcp.pending_received = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-pendingSend" => {
                    stats.proc.tcp.pending_send = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-receivedBytesSinceLastRun" => {
                    stats.proc.tcp.received_bytes_since_last_run = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-receivedBytesTotal" => {
                    stats.proc.tcp.received_bytes_total = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-sentBytesSinceLastRun" => {
                    stats.proc.tcp.sent_bytes_since_last_run = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "proc-tcp-sentBytesTotal" => {
                    stats.proc.tcp.sent_bytes_total = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "sys-loadavg-1m" => {
                    stats.sys.loadavg.one_m = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "sys-loadavg-5m" => {
                    stats.sys.loadavg.five_m = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "sys-loadavg-15m" => {
                    stats.sys.loadavg.fifteen_m = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "sys-freeMem" => {
                    // It's possible we got a negative value here, hence why we try to parse a i64 first.
                    match value.parse::<i64>() {
                        Ok(value) => {
                            if value < 0 {
                                stats.sys.free_mem = 0;
                            } else {
                                stats.sys.free_mem = value as usize;
                            }
                        }

                        Err(e) => match e.kind() {
                            IntErrorKind::NegOverflow => {
                                stats.sys.free_mem = 0;
                            }

                            IntErrorKind::PosOverflow => {
                                // In case the number is too big for a signed number, we try unsigned.
                                stats.sys.free_mem = value.parse().map_err(|e| {
                                    kurrent::Error::InternalParsingError(format!(
                                        "{key}: {err} = '{value}'",
                                        key = key,
                                        err = e,
                                        value = value,
                                    ))
                                })?;
                            }

                            _ => {
                                return Err(kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                )));
                            }
                        },
                    }
                }

                "es-checksum" => {
                    stats.es.checksum = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-checksumNonFlushed" => {
                    stats.es.checksum_non_flushed = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                key_str if key_str.starts_with("sys-drive-") => {
                    if stats.sys.drive.is_none() {
                        let (path, _) = key_str
                            .strip_prefix("sys-drive-")
                            .unwrap()
                            .split_once('-')
                            .unwrap();

                        stats.sys.drive = Some(Drive {
                            path: path.to_string(),
                            stats: Default::default(),
                        });
                    }

                    let drive = stats.sys.drive.as_mut().unwrap();
                    let (_, prop) = key_str.rsplit_once('-').unwrap();

                    match prop {
                        "availableBytes" => {
                            drive.stats.available_bytes = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "totalBytes" => {
                            drive.stats.total_bytes = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "usage" => {
                            drive.stats.usage = value;
                        }

                        "usedBytes" => {
                            drive.stats.used_bytes = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        _ => {
                            warn!("Unknown drive metric: '{}'", prop);
                        }
                    }
                }

                key_str if key_str.starts_with("es-queue-") => {
                    let (queue_name, prop) = key_str
                        .strip_prefix("es-queue-")
                        .unwrap()
                        .rsplit_once('-')
                        .unwrap();

                    let queue = stats.es.queues.entry(queue_name.to_string()).or_default();

                    match prop {
                        "queueName" => {
                            queue.name = value;
                        }

                        "groupName" => {
                            queue.group_name = value;
                        }

                        "avgItemsPerSecond" => {
                            queue.avg_items_per_second = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "avgProcessingTime" => {
                            queue.avg_processing_time = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "currentIdleTime" => {
                            if value == "null" {
                                continue;
                            }

                            queue.current_idle_time = Some(value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?);
                        }

                        "currentItemProcessingTime" => {
                            if value == "null" {
                                continue;
                            }

                            queue.current_item_processing_time =
                                Some(value.parse().map_err(|e| {
                                    kurrent::Error::InternalParsingError(format!(
                                        "{key}: {err} = '{value}'",
                                        key = key,
                                        err = e,
                                        value = value,
                                    ))
                                })?);
                        }

                        "idleTimePercent" => {
                            queue.idle_time_percent = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "length" => {
                            queue.length = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "lengthCurrentTryPeak" => {
                            queue.length_current_try_peak = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "lengthLifetimePeak" => {
                            queue.length_lifetime_peak = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "totalItemsProcessed" => {
                            queue.total_items_processed = value.parse().map_err(|e| {
                                kurrent::Error::InternalParsingError(format!(
                                    "{key}: {err} = '{value}'",
                                    key = key,
                                    err = e,
                                    value = value,
                                ))
                            })?;
                        }

                        "inProgressMessage" => {
                            queue.in_progress_message = value;
                        }

                        "lastProcessedMessage" => {
                            queue.last_processed_message = value;
                        }

                        _ => {
                            warn!("Unknown queue metric: '{}'", key);
                        }
                    }
                }

                "es-writer-lastFlushSize" => {
                    stats.es.writer.last_flush_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-lastFlushDelayMs" => {
                    stats.es.writer.last_flush_delays_ms = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-meanFlushSize" => {
                    stats.es.writer.mean_flush_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-meanFlushDelayMs" => {
                    stats.es.writer.mean_flush_delays_ms = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-maxFlushSize" => {
                    stats.es.writer.max_flush_size = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-maxFlushDelayMs" => {
                    stats.es.writer.max_flush_delays_ms = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-writer-queuedFlushMessages" => {
                    stats.es.writer.queued_flush_messages = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-cachedRecord" => {
                    stats.es.read_index.cached_record = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-notCachedRecord" => {
                    stats.es.read_index.not_cached_record = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-cachedStreamInfo" => {
                    stats.es.read_index.cached_stream_info = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-notCachedStreamInfo" => {
                    stats.es.read_index.not_cached_stream_info = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-cachedTransInfo" => {
                    stats.es.read_index.cached_trans_info = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                "es-readIndex-notCachedTransInfo" => {
                    stats.es.read_index.not_cached_trans_info = value.parse().map_err(|e| {
                        kurrent::Error::InternalParsingError(format!(
                            "{key}: {err} = '{value}'",
                            key = key,
                            err = e,
                            value = value,
                        ))
                    })?;
                }

                _ => {
                    warn!("Unknown stat metric: '{}'", key);
                }
            }
        }

        Ok(stats)
    }
}

#[derive(Debug, Clone)]
pub struct Proc {
    pub id: i64,
    pub start_time: DateTime<Utc>,
    pub mem: i64,
    pub cpu: f64,
    pub threads_count: i64,
    pub thrown_exceptions_rate: f64,
    pub contentions_rate: f64,
    pub gc: Gc,
    pub disk_io: DiskIo,
    pub tcp: Tcp,
}

impl Default for Proc {
    fn default() -> Self {
        Self {
            id: 0,
            start_time: std::time::UNIX_EPOCH.into(),
            mem: 0,
            cpu: 0.0,
            threads_count: 0,
            thrown_exceptions_rate: 0.0,
            contentions_rate: 0.0,
            gc: Default::default(),
            disk_io: Default::default(),
            tcp: Default::default(),
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct Gc {
    pub allocation_speed: f64,
    pub gen0_items_count: i64,
    pub gen0_size: i64,
    pub gen1_items_count: i64,
    pub gen1_size: i64,
    pub gen2_items_count: i64,
    pub gen2_size: i64,
    pub large_heap_size: i64,
    pub time_in_gc: f64,
    pub total_bytes_in_heaps: i64,
}

#[derive(Debug, Default, Clone)]
pub struct Tcp {
    pub connections: i64,
    pub receiving_speed: f64,
    pub sending_speed: f64,
    pub in_send: i64,
    pub measure_time: String,
    pub pending_received: i64,
    pub pending_send: i64,
    pub received_bytes_since_last_run: i64,
    pub received_bytes_total: i64,
    pub sent_bytes_since_last_run: i64,
    pub sent_bytes_total: i64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DiskIo {
    pub read_bytes: i64,
    pub written_bytes: i64,
    pub read_ops: i64,
    pub write_ops: i64,
}

#[derive(Debug, Default, Clone)]
pub struct Sys {
    pub free_mem: usize,
    pub loadavg: LoadAvg,
    pub drive: Option<Drive>,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct LoadAvg {
    pub one_m: f64,
    pub five_m: f64,
    pub fifteen_m: f64,
}

#[derive(Debug, Default, Clone)]
pub struct Es {
    pub queues: BTreeMap<String, Queue>,
    pub checksum: i64,
    pub checksum_non_flushed: i64,
    pub writer: Writer,
    pub read_index: ReadIndex,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct Writer {
    pub last_flush_size: i64,
    pub last_flush_delays_ms: f64,
    pub mean_flush_size: i64,
    pub mean_flush_delays_ms: f64,
    pub max_flush_size: i64,
    pub max_flush_delays_ms: f64,
    pub queued_flush_messages: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReadIndex {
    pub cached_record: i64,
    pub not_cached_record: i64,
    pub cached_stream_info: i64,
    pub not_cached_stream_info: i64,
    pub cached_trans_info: i64,
    pub not_cached_trans_info: i64,
}

#[derive(Debug, Clone, Default)]
pub struct Queue {
    pub name: String,
    pub group_name: String,
    pub avg_items_per_second: usize,
    pub current_idle_time: Option<String>,
    pub current_item_processing_time: Option<String>,
    pub idle_time_percent: f32,
    pub length_current_try_peak: i64,
    pub length_lifetime_peak: i64,
    pub length: i64,
    pub avg_processing_time: f64,
    pub total_items_processed: i64,
    pub in_progress_message: String,
    pub last_processed_message: String,
}

#[derive(Debug, Clone, Default)]
pub struct Drive {
    pub path: String,
    pub stats: DriveStats,
}

#[derive(Debug, Clone, Default)]
pub struct DriveStats {
    pub available_bytes: usize,
    pub total_bytes: usize,
    pub usage: String,
    pub used_bytes: usize,
}
