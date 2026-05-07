import os
import re
import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, expr, regexp_extract,
    current_timestamp, input_file_name, monotonically_increasing_id,
    to_date, quarter as spark_quarter, sum as spark_sum, count as spark_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, BooleanType, TimestampType
)

@dataclass
class PipelineConfig:
    source_path: str = "/Workspace/Users/artisadikaj99@gmail.com/project/"
    staging_path: str = "/Volumes/workspace/default/main/output/staging"
    curated_path: str = "/Volumes/workspace/default/main/output/curated"
    checkpoint_path: str = "/Volumes/workspace/default/main/output/checkpoints"
    logs_path: str = "/Volumes/workspace/default/main/output/logs"

    file_pattern: str = "sales_*.csv"
    output_format: str = "parquet"
    partition_columns: List[str] = field(default_factory=lambda: ["year", "month"])

    identifier_columns: List[str] = field(default_factory=lambda: [
        "salesperson_id", "salesperson_name",
        "sales_state_id", "sales_state_name",
        "store_id", "store_name", "year"
    ])
    sales_column_pattern: str = r"^([A-Z][a-z]{2})_sales$"

    enable_validation: bool = True
    reject_negative_sales: bool = True
    max_sales_value: float = 1_000_000_000
    enable_incremental: bool = True
    enable_schema_registry: bool = True
    enable_deduplication: bool = True

def setup_logging(config: PipelineConfig) -> logging.Logger:
    os.makedirs(config.logs_path, exist_ok=True)

    log_file = os.path.join(
        config.logs_path,
        f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger("SalesPipeline")
    logger.info(f"Logging initialized. Log file: {log_file}")

    return logger


class SchemaRegistry:
    def __init__(self, registry_path: str):
        self.registry_path = registry_path
        self.registry_file = os.path.join(registry_path, "schema_registry.json")
        self.schemas: Dict[str, Dict] = self._load_registry()

    def _load_registry(self) -> Dict:
        os.makedirs(self.registry_path, exist_ok=True)

        if os.path.exists(self.registry_file):
            with open(self.registry_file, 'r') as f:
                return json.load(f)
        return {"schemas": {}, "drift_events": []}

    def _save_registry(self):
        with open(self.registry_file, 'w') as f:
            json.dump(self.schemas, f, indent=2, default=str)

    def register_schema(self, file_name: str, columns: List[str],
                       identifier_cols: List[str], sales_cols: List[str]) -> bool:
        schema_signature = {
            "columns": sorted(columns),
            "identifier_columns": sorted(identifier_cols),
            "sales_columns": sorted(sales_cols),
            "column_count": len(columns),
            "first_seen": datetime.now().isoformat()
        }

        schema_key = f"cols_{len(columns)}_sales_{len(sales_cols)}"

        is_new = schema_key not in self.schemas.get("schemas", {})

        if is_new:
            self.schemas.setdefault("schemas", {})[schema_key] = schema_signature
            self.schemas.setdefault("drift_events", []).append({
                "timestamp": datetime.now().isoformat(),
                "file": file_name,
                "event": "new_schema_detected",
                "schema_key": schema_key,
                "new_columns": sales_cols
            })
            self._save_registry()

        return is_new

    def get_all_known_sales_columns(self) -> List[str]:
        all_sales_cols = set()
        for schema in self.schemas.get("schemas", {}).values():
            all_sales_cols.update(schema.get("sales_columns", []))
        return sorted(all_sales_cols)


class CheckpointManager:
    def __init__(self, checkpoint_path: str):
        self.checkpoint_path = checkpoint_path
        self.checkpoint_file = os.path.join(checkpoint_path, "processed_files.json")
        self.processed_files: Dict = self._load_checkpoint()

    def _load_checkpoint(self) -> Dict:
        os.makedirs(self.checkpoint_path, exist_ok=True)

        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {"files": {}, "last_run": None}

    def _save_checkpoint(self):
        self.processed_files["last_run"] = datetime.now().isoformat()
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.processed_files, f, indent=2)

    def is_processed(self, file_path: str) -> bool:
        file_name = os.path.basename(file_path)
        file_stat = os.stat(file_path)
        file_key = f"{file_name}_{file_stat.st_size}_{file_stat.st_mtime}"

        return file_key in self.processed_files.get("files", {})

    def mark_processed(self, file_path: str, record_count: int, status: str = "success"):
        file_name = os.path.basename(file_path)
        file_stat = os.stat(file_path)
        file_key = f"{file_name}_{file_stat.st_size}_{file_stat.st_mtime}"

        self.processed_files.setdefault("files", {})[file_key] = {
            "file_name": file_name,
            "processed_at": datetime.now().isoformat(),
            "record_count": record_count,
            "status": status
        }
        self._save_checkpoint()


class DataValidator:
    def __init__(self, config: PipelineConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.metrics: Dict = {}

    def validate_dataframe(self, df: DataFrame, file_name: str) -> Tuple[DataFrame, DataFrame]:
        validated_df = df.withColumn(
            "is_valid",
            lit(True)
        )

        for col_name in self.config.identifier_columns:
            if col_name in df.columns:
                validated_df = validated_df.withColumn(
                    "is_valid",
                    when(col(col_name).isNull(), lit(False)).otherwise(col("is_valid"))
                )

        if "sales_amount" in validated_df.columns:
            if self.config.reject_negative_sales:
                validated_df = validated_df.withColumn(
                    "is_valid",
                    when(col("sales_amount") < 0, lit(False)).otherwise(col("is_valid"))
                )

            validated_df = validated_df.withColumn(
                "is_valid",
                when(col("sales_amount") > self.config.max_sales_value, lit(False))
                .otherwise(col("is_valid"))
            )

        valid_df = validated_df.filter(col("is_valid") == True)
        invalid_df = validated_df.filter(col("is_valid") == False)

        total_count = df.count()
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        self.metrics[file_name] = {
            "total_records": total_count,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "validation_rate": (valid_count / total_count * 100) if total_count > 0 else 0
        }

        self.logger.info(
            f"Validation complete for {file_name}: "
            f"{valid_count}/{total_count} valid ({self.metrics[file_name]['validation_rate']:.2f}%)"
        )

        return valid_df.drop("is_valid"), invalid_df


class SalesDataPipeline:
    MONTH_MAP = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = setup_logging(config)
        self.spark = self._create_spark_session()

        self.schema_registry = SchemaRegistry(config.checkpoint_path)
        self.checkpoint_manager = CheckpointManager(config.checkpoint_path)
        self.validator = DataValidator(config, self.logger)

        for path in [config.staging_path, config.curated_path]:
            os.makedirs(path, exist_ok=True)

        self.logger.info("Pipeline initialized successfully")

    def _create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName("SalesDataPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()


        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)
        return spark

    def discover_files(self) -> List[str]:
        import glob

        pattern = os.path.join(self.config.source_path, self.config.file_pattern)
        files = glob.glob(pattern)

        self.logger.info(f"Discovered {len(files)} files matching pattern '{self.config.file_pattern}'")

        return files

    def classify_columns(self, columns: List[str]) -> Tuple[List[str], List[str]]:
        identifier_cols = []
        sales_cols = []

        pattern = re.compile(self.config.sales_column_pattern)

        for col_name in columns:
            if pattern.match(col_name):
                sales_cols.append(col_name)
            else:
                identifier_cols.append(col_name)

        self.logger.info(
            f"Column classification: {len(identifier_cols)} identifiers, {len(sales_cols)} sales columns"
        )

        return identifier_cols, sales_cols

    def read_csv(self, file_path: str) -> DataFrame:
        self.logger.info(f"Reading file: {file_path}")

        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        df = df.withColumn("source_file", lit(os.path.basename(file_path))) \
               .withColumn("load_timestamp", current_timestamp())

        return df

    def unpivot_sales_columns(self, df: DataFrame, identifier_cols: List[str],
                              sales_cols: List[str]) -> DataFrame:
        if not sales_cols:
            self.logger.warning("No sales columns found to unpivot")
            return df

        stack_exprs = []
        for col_name in sales_cols:
            month = col_name.replace("_sales", "")
            stack_exprs.append(f"'{month}', `{col_name}`")

        stack_expr = f"stack({len(sales_cols)}, {', '.join(stack_exprs)}) as (month, sales_amount)"
        select_cols = identifier_cols + ["source_file", "load_timestamp"]

        unpivoted_df = df.select(
            *[col(c) for c in select_cols],
            expr(stack_expr)
        )

        month_map_expr = " ".join([
            f"WHEN month = '{m}' THEN {n}"
            for m, n in self.MONTH_MAP.items()
        ])

        unpivoted_df = unpivoted_df \
            .withColumn("month_number", expr(f"CASE {month_map_expr} ELSE 0 END")) \
            .withColumn("quarter", ((col("month_number") - 1) / 3 + 1).cast("int"))

        self.logger.info(f"Unpivoted {len(sales_cols)} sales columns into rows")

        return unpivoted_df

    def process_file(self, file_path: str, incremental: bool = True) -> Optional[DataFrame]:
        file_name = os.path.basename(file_path)

        if incremental and self.config.enable_incremental:
            if self.checkpoint_manager.is_processed(file_path):
                self.logger.info(f"Skipping already processed file: {file_name}")
                return None

        try:
            df = self.read_csv(file_path)
            record_count = df.count()
            self.logger.info(f"Read {record_count} records from {file_name}")

            columns = [c for c in df.columns if c not in ["source_file", "load_timestamp"]]
            identifier_cols, sales_cols = self.classify_columns(columns)

            if self.config.enable_schema_registry:
                is_new_schema = self.schema_registry.register_schema(
                    file_name, columns, identifier_cols, sales_cols
                )
                if is_new_schema:
                    self.logger.warning(f"New schema detected in {file_name}")

            transformed_df = self.unpivot_sales_columns(df, identifier_cols, sales_cols)

            if self.config.enable_validation:
                valid_df, invalid_df = self.validator.validate_dataframe(
                    transformed_df, file_name
                )

                if invalid_df.count() > 0:
                    invalid_path = os.path.join(
                        self.config.staging_path,
                        "invalid_records",
                        file_name.replace(".csv", "")
                    )
                    invalid_df.write.mode("overwrite").parquet(invalid_path)
                    self.logger.warning(f"Saved {invalid_df.count()} invalid records to {invalid_path}")

                transformed_df = valid_df

            if self.config.enable_deduplication:
                before_count = transformed_df.count()
                transformed_df = transformed_df.dropDuplicates(
                    identifier_cols + ["month"]
                )
                after_count = transformed_df.count()
                if before_count != after_count:
                    self.logger.info(f"Removed {before_count - after_count} duplicate records")

            self.checkpoint_manager.mark_processed(file_path, record_count, "success")

            return transformed_df

        except Exception as e:
            self.logger.error(f"Error processing {file_name}: {str(e)}")
            self.checkpoint_manager.mark_processed(file_path, 0, f"error: {str(e)}")
            raise

    def write_output(self, df: DataFrame, mode: str = "append"):
        output_path = os.path.join(self.config.curated_path, "sales_normalized")

        writer = df.write \
            .mode(mode) \
            .format(self.config.output_format)

        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)

        writer.save(output_path)

        self.logger.info(f"Written output to {output_path}")

    def run(self, incremental: bool = True) -> Dict:
        start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE EXECUTION STARTED")
        self.logger.info("=" * 60)

        metrics = {
            "start_time": start_time.isoformat(),
            "files_discovered": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "total_records": 0,
            "errors": []
        }

        try:
            files = self.discover_files()
            metrics["files_discovered"] = len(files)

            if not files:
                self.logger.warning("No files found to process")
                return metrics

            all_dfs = []
            for file_path in files:
                try:
                    result_df = self.process_file(file_path, incremental)
                    if result_df is not None:
                        all_dfs.append(result_df)
                        metrics["files_processed"] += 1
                        metrics["total_records"] += result_df.count()
                    else:
                        metrics["files_skipped"] += 1
                except Exception as e:
                    metrics["errors"].append({
                        "file": file_path,
                        "error": str(e)
                    })

            if all_dfs:
                from functools import reduce
                combined_df = reduce(lambda a, b: a.union(b), all_dfs)
                self.write_output(combined_df, mode="overwrite" if not incremental else "append")

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            metrics["errors"].append({"pipeline": str(e)})
            raise

        finally:
            end_time = datetime.now()
            metrics["end_time"] = end_time.isoformat()
            metrics["duration_seconds"] = (end_time - start_time).total_seconds()

            self.logger.info("=" * 60)
            self.logger.info("PIPELINE EXECUTION COMPLETED")
            self.logger.info(f"Duration: {metrics['duration_seconds']:.2f} seconds")
            self.logger.info(f"Files processed: {metrics['files_processed']}/{metrics['files_discovered']}")
            self.logger.info(f"Total records: {metrics['total_records']}")
            self.logger.info("=" * 60)

            metrics_path = os.path.join(
                self.config.logs_path,
                f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(metrics_path, 'w') as f:
                json.dump(metrics, f, indent=2)

        return metrics

    def stop(self):
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


def main():
    config = PipelineConfig(
        source_path="/Workspace/Users/artisadikaj99@gmail.com/project/",
        staging_path="/Volumes/workspace/default/main/output/staging",
        curated_path="/Volumes/workspace/default/main/output/curated",
        checkpoint_path="/Volumes/workspace/default/main/output/checkpoints",
        logs_path="/Volumes/workspace/default/main/output/logs"
    )

    pipeline = SalesDataPipeline(config)

    try:
        metrics = pipeline.run(incremental=False)

        print("\n" + "=" * 60)
        print("EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Files Processed: {metrics['files_processed']}")
        print(f"Total Records: {metrics['total_records']}")
        print(f"Duration: {metrics['duration_seconds']:.2f} seconds")

        if metrics['errors']:
            print(f"Errors: {len(metrics['errors'])}")
            for err in metrics['errors']:
                print(f"  - {err}")

        print("=" * 60)

    finally:
        pipeline.stop()

if __name__ == "__main__":
    main()