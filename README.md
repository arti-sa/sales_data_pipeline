# Sales Data Pipeline

A robust, scalable data pipeline for processing sales data with dynamic schema handling.

## Overview

This pipeline addresses the challenge of processing sales data files with evolving schemas. Files may contain different numbers of monthly sales columns (e.g., Jan-Mar, Jan-Jul, or full year), and the pipeline handles these variations automatically without code changes.

## Features

- **Dynamic Schema Detection**: Automatically identifies identifier vs. sales columns
- **Schema Evolution**: Handles new monthly columns without manual intervention
- **Normalized Output**: Transforms wide format to long format for consistent analytics
- **Data Validation**: Validates data quality and separates invalid records
- **Incremental Processing**: Tracks processed files to avoid reprocessing
- **Schema Registry**: Logs schema changes for drift detection
- **Comprehensive Logging**: Structured logs with execution metrics

## Project Structure

```
pipeline/
├── sales_pipeline.py       # Main PySpark pipeline
├── sql_transformations.sql # SQL alternatives for ADF/Fabric
├── config.yaml             # Configuration settings
├── requirements.txt        # Python dependencies
└── README.md               # This file

output/                     # Generated after running
├── staging/                # Intermediate data
├── curated/                # Final normalized data (Parquet)
├── checkpoints/            # Processing state
└── logs/                   # Execution logs
```

## Quick Start

### Prerequisites

- Python 3.8+
- Java 8 or 11 (for Spark)
- Apache Spark 3.3+

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install PySpark directly
pip install pyspark>=3.3.0
```

### Running the Pipeline

```bash
# Navigate to the project directory
cd /path/to/arti

# Run the pipeline
python pipeline/sales_pipeline.py
```

### Expected Output

```
PIPELINE EXECUTION STARTED
Discovered 3 files matching pattern 'sales_*.csv'
Read 100000 records from sales_2025_jan_mar_100k.csv
Column classification: 7 identifiers, 3 sales columns
Unpivoted 3 sales columns into rows
...
PIPELINE EXECUTION COMPLETED
Duration: X.XX seconds
Files processed: 3/3
Total records: 2,200,000
```

## Data Flow

```
[CSV Files with Variable Schemas]
         |
         v
[Dynamic Column Detection]
         |
         v
[Unpivot Transformation]
    Wide → Long format
         |
         v
[Data Validation]
         |
         v
[Parquet Output]
    Partitioned by year/month
```

## Input Schema (Wide Format)

| Column | Type | Description |
|--------|------|-------------|
| salesperson_id | INT | Salesperson identifier |
| salesperson_name | STRING | Salesperson name |
| sales_state_id | INT | State identifier |
| sales_state_name | STRING | State name |
| store_id | INT | Store identifier |
| store_name | STRING | Store name |
| year | INT | Sales year |
| {Month}_sales | DECIMAL | Monthly sales (variable) |

## Output Schema (Normalized)

| Column | Type | Description |
|--------|------|-------------|
| source_file | STRING | Source CSV filename |
| load_timestamp | TIMESTAMP | Processing timestamp |
| salesperson_id | INT | Salesperson identifier |
| salesperson_name | STRING | Salesperson name |
| sales_state_id | INT | State identifier |
| sales_state_name | STRING | State name |
| store_id | INT | Store identifier |
| store_name | STRING | Store name |
| year | INT | Sales year |
| month | STRING | Month name (Jan, Feb, etc.) |
| month_number | INT | Month number (1-12) |
| sales_amount | DECIMAL | Sales value |
| quarter | INT | Quarter (1-4) |

## Configuration

Edit `config.yaml` to customize:

```yaml
paths:
  source: "."           # CSV file location
  curated: "./output"   # Output location

features:
  enable_incremental: true      # Skip already processed files
  enable_schema_registry: true  # Track schema changes
  enable_validation: true       # Validate data quality
```

## Azure Integration

### Microsoft Fabric / Azure Data Factory

Use the SQL transformations in `sql_transformations.sql` for:

1. **Dataflow**: Use the UNPIVOT logic in a Data Flow activity
2. **Stored Procedure**: Deploy SQL scripts to Synapse/SQL Server
3. **Notebook**: Convert PySpark code to Fabric notebook

### Deployment Steps

1. Upload CSV files to Azure Data Lake Storage
2. Create a Fabric Lakehouse or Synapse database
3. Deploy pipeline using preferred method (Notebook, Dataflow, or ADF)
4. Schedule pipeline for regular execution

## Monitoring

The pipeline generates:

- **Execution Logs**: `output/logs/pipeline_*.log`
- **Metrics**: `output/logs/metrics_*.json`
- **Schema Registry**: `output/checkpoints/schema_registry.json`

Sample metrics:

```json
{
  "files_processed": 3,
  "total_records": 2200000,
  "duration_seconds": 45.2,
  "errors": []
}
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Memory errors | Increase Spark driver memory: `--driver-memory 4g` |
| Slow processing | Enable adaptive query execution (default) |
| Missing columns | Check sales column pattern in config |
| File not found | Verify source_path in config |

## License

Internal use only.
