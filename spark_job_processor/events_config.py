events_config = {
    "SparkListenerApplicationStart": {
        "application_start_time": ["Timestamp"]
    },
    "SparkListenerApplicationEnd": {
        "application_end_time": ["Timestamp"]
    },
    "SparkListenerExecutorAdded": {
        "executor_id": ["Executor ID"],
        "executor_start_time": ["Timestamp"],
        "cores_num": ["Executor Info", "Total Cores"]
    },
    "SparkListenerTaskEnd": {
        "executor_id": ["Task Info", "Executor ID"],
        "executor_cpu_time": ["Task Metrics", "Executor CPU Time"],
        "bytes_read": ["Task Metrics", "Input Metrics", "Bytes Read"],
        "records_read": ["Task Metrics", "Input Metrics", "Records Read"],
        "bytes_written": ["Task Metrics", "Output Metrics", "Bytes Written"],
        "records_written": ["Task Metrics", "Output Metrics", "Records Written"],
        "local_bytes_read": ["Task Metrics", "Shuffle Read Metrics", "Local Bytes Read"],
        "remote_bytes_read": ["Task Metrics", "Shuffle Read Metrics", "Remote Bytes Read"],
        "shuffle_bytes_written": ["Task Metrics", "Shuffle Write Metrics", "Shuffle Bytes Written"],
        "jvm_memory": ["Task Executor Metrics", "ProcessTreeJVMRSSMemory"],
        "python_memory": ["Task Executor Metrics", "ProcessTreePythonRSSMemory"],
        "other_memory": ["Task Executor Metrics", "ProcessTreeOtherRSSMemory"]
    },
    "SparkListenerExecutorRemoved": {
        "executor_id": ["Executor ID"],
        "executor_end_time": ["Timestamp"]
    },
    "SparkListenerExecutorCompleted": {
        "executor_id": ["Executor ID"],
        "executor_end_time": ["Timestamp"]
    },
    "SparkListenerEnvironmentUpdate": {
        "executor_memory": ["Spark Properties", "spark.executor.memory"],
        "memory_overhead_factor": ["Spark Properties", "spark.yarn.executor.memoryOverheadFactor"]

    }
}
