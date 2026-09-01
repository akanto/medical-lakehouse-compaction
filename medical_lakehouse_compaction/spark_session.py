import os
from pyspark.sql import SparkSession
from medical_lakehouse_compaction.metrics.stage_log import event_log_configs

ICEBERG_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

def create_spark_session(config: dict, app_name: str = "hybrid-cloud-medical-imaging") -> SparkSession:
    # Local mode: the driver JVM is the whole cluster. Profiles override this
    # for long campaign runs (experiment.yaml: 14g on the 21 GB c5n.2xlarge) —
    # the 200-series grid OOMed at 8g after ~2 h of accumulated driver state
    # (S1 queries plan ~1,100 tasks each; W2 issues 20 per repetition).
    driver_memory = config.get("driver_memory", "8g")
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {driver_memory} --packages {ICEBERG_PACKAGES} pyspark-shell")

    # Spark always runs in local mode here (single JVM), so the in-process
    # executor must reach the driver over loopback. Without this, Spark picks
    # the host's LAN IP for spark.driver.host; if that address changes (laptop
    # switches network) or is firewalled, the executor times out connecting back
    # and SparkContext init fails. Pin to 127.0.0.1 unless the operator overrides.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.dicom",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.dicom.type", "hadoop")
        .config("spark.sql.catalog.dicom.warehouse", config["warehouse"])
    )

    if config.get("endpoint"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", config["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", config["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", config["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    # Stage-level telemetry. Off unless the profile sets event_log_dir, because
    # the log is only needed when stage records are wanted; the UI stays off
    # either way. See medical_lakehouse_compaction/metrics/stage_log.py for why the UI is not an option.
    for k, v in event_log_configs(config.get("event_log_dir")).items():
        builder = builder.config(k, v)

    spark = (
        builder
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.sql.adaptive.enabled", "true")
        # Benchmark sessions run headless for hours and issue tens of
        # thousands of stages; the default UI/event retention grows driver
        # heap without bound. Retention caps do not affect S3A IOStatistics.
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ui.retainedExecutions", "10")
        .config("spark.ui.retainedJobs", "100")
        .config("spark.ui.retainedStages", "100")
        .config("spark.ui.retainedTasks", "1000")
        .getOrCreate()
    )
    _suppress_noisy_loggers(spark)
    return spark


def _suppress_noisy_loggers(spark: SparkSession) -> None:
    """Silence expected dev-environment noise that would otherwise pollute output."""
    noisy = [
        # Iceberg HadoopCatalog looks for version-hint before creating a new table;
        # FileNotFoundException at that point is normal — table just doesn't exist yet.
        "org.apache.iceberg.hadoop.HadoopTableOperations",
        # macOS has no native Hadoop library; builtin Java fallback works fine.
        "org.apache.hadoop.util.NativeCodeLoader",
        # No hadoop-metrics2 config file in dev; metrics are unused here.
        "org.apache.hadoop.metrics2.impl.MetricsConfig",
    ]
    try:
        jvm = spark.sparkContext._jvm
        Configurator = jvm.org.apache.logging.log4j.core.config.Configurator
        Level = jvm.org.apache.logging.log4j.Level
        for name in noisy:
            Configurator.setLevel(name, Level.ERROR)
    except Exception:
        pass
