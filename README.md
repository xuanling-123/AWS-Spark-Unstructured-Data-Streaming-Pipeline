# AWS-Spark-Unstructured-Data-Streaming-Pipeline
Real-time streaming ETL pipeline processing unstructured job postings (text/JSON/CSV/PDF/images) using Apache Spark, Docker, and AWS services (S3, Glue, Athena, Lake Formation) with automated schema discovery and cataloging.

## 💡 Why Unstructured Data Matters

**80-90% of enterprise data is unstructured**—text documents, emails, PDFs, images, and social media—yet most organizations struggle to extract value from it. This project demonstrates how to transform chaotic, unstructured job postings from **5 different formats** (text, JSON, CSV, PDF, images) into structured, queryable data using real-time streaming, enabling instant insights into hiring trends, salary benchmarks, and market intelligence. Mastering unstructured data processing is now a **critical competitive advantage** as AI and automation reshape business operations.


# 🚀 AWS Spark Unstructured Data Streaming Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-orange.svg)](https://aws.amazon.com/)
[![OCR](https://img.shields.io/badge/OCR-Tesseract-green.svg)](https://github.com/tesseract-ocr/tesseract)

## 📋 Project Overview

A **production-ready real-time streaming ETL pipeline** that processes unstructured job posting data from **multiple formats** (text, JSON, CSV, PDF, and images) using Apache Spark Structured Streaming. The pipeline extracts, transforms, and loads data into AWS S3, with automated schema discovery via AWS Glue Crawler and query capabilities through AWS Athena.

### 🎯 Key Features

- ✅ **Real-time Streaming**: Processes new files as they arrive using Spark Structured Streaming
- ✅ **Multi-format Support**: Handles 5 different formats:
  - 📄 **Unstructured Text** (.txt) - Raw text with regex extraction
  - 📋 **JSON** - Semi-structured data with nested fields
  - 📊 **CSV** - Structured tabular data
  - 📑 **PDF** - Document extraction using PyMuPDF
  - 🖼️ **Images** (.jpg, .jpeg, .png) - OCR text extraction using Tesseract
- ✅ **Advanced Text Extraction**: Custom UDF-based parsing with OCR capabilities
- ✅ **AWS Integration**: Full integration with S3, Glue, Athena, and Lake Formation
- ✅ **Containerized**: Fully Dockerized Spark cluster (1 master + 2 workers)
- ✅ **Schema Evolution**: Automatic schema detection and cataloging via AWS Glue
- ✅ **Dual Mode**: Supports both local development and AWS production modes
- ✅ **Image Preprocessing**: Enhanced OCR accuracy with PIL image optimization

## 🏗️ Architecture

<img width="1274" height="401" alt="image" src="https://github.com/user-attachments/assets/91503a14-e792-4c1d-a45f-db274665f8af" />


### Data Flow

1. **Source Data**: Multiple formats (text, JSON, CSV, PDF, images)
2. **Spark Streaming**: Monitors 5 input directories for new files
3. **Transformation**: 
   - Custom UDFs extract structured fields
   - OCR for image-based job postings
   - PDF text extraction
4. **Storage**: Writes Parquet files to S3 with checkpointing
5. **Cataloging**: AWS Glue Crawler discovers schema and creates tables
6. **Query Layer**: AWS Athena enables SQL queries on processed data

```
┌──────────────────────────────────────────────────────────────────┐
│                    Input Sources (5 Types)                       │
├─────────────┬─────────────┬─────────────┬─────────────┬──────────┤
│  📄 Text    │   📋 JSON    │  📊 CSV     │  📑 PDF     │ 🖼️ Image  │
│  (.txt)     │  (.json)    │  (.csv)     │  (.pdf)     │ (.jpg)   │
└─────────────┴─────────────┴─────────────┴─────────────┴──────────┘
       │              │              │              │         │
       ▼              ▼              ▼              ▼         ▼
┌──────────────────────────────────────────────────────────────┐
│           Spark Structured Streaming (5 Streams)             │
│  • Regex Extraction  • JSON Parsing  • CSV Schema            │
│  • PyMuPDF          • Tesseract OCR + Image Preprocessing    │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │     AWS S3      │
                    │   (Parquet)     │
                    └─────────────────┘
                              │
                              ▼
┌─────────────────┐       ┌────────────────────┐       ┌─────────────────┐
│  AWS Athena     │◀──────│  AWS Glue Crawler  │◀──────│  Lake Formation │
│  (SQL Queries)  │       │  (Schema Discovery)│       │  (Permissions)  │
└─────────────────┘       └────────────────────┘       └─────────────────┘
```

## 📊 Sample Data Processing

### Input: Unstructured Text
```
DevOps Engineer - Lead Position

Job ID: 7291
Posted: 03/10/2024
Salary Range: $135,000 - $175,000

REQUIRED QUALIFICATIONS:
- 5+ years of experience with AWS cloud services
- Strong knowledge of CI/CD pipelines...
```

### Output: Structured Parquet
| file_name | position | classcode | salary_start | salary_end | start_date | source_type | ... |
|-----------|----------|-----------|--------------|------------|------------|-------------|-----|
| devops_engineer_7291 | DevOps Engineer - Lead | 7291 | 135000.0 | 175000.0 | 2024-03-10 | TEXT | ... |
| data_scientist_5823 | Senior Data Scientist | 5823 | 120000.0 | 160000.0 | 2024-02-15 | JSON | ... |
| cloud_architect_8901 | Cloud Solutions Architect | 8901 | 145000.0 | 185000.0 | NULL | CSV | ... |
| ml_engineer_6745 | Machine Learning Engineer | 6745 | 130000.0 | 170000.0 | 2024-03-01 | PDF | ... |
| senior_data_engineer_9876 | Senior Data Engineer | 9876 | 95000.0 | 125000.0 | 2025-02-10 | IMAGE | ... |

## 🚀 Getting Started

### Prerequisites

- Docker Desktop installed and running
- AWS Account with configured credentials
- Python 3.8+ (for local development)
- AWS CLI configured

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/AWS-Spark-Unstructured-Data-Streaming-Pipeline.git
cd AWS-Spark-Unstructured-Data-Streaming-Pipeline
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

`.env` file structure:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name
```

3. **Build Docker containers**
```bash
docker compose build
docker compose up -d
```

4. **Download required JARs (for S3 access)**
```bash
cd jobs
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
cd ..
```

### Running the Pipeline

**Option 1: Local Mode (no S3)**
```bash
# Leave AWS credentials empty in .env
docker exec -it aws_spark_unstructured-spark-master-1 /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark/jobs/main.py
```

**Option 2: AWS S3 Mode**
```bash
docker exec -it aws_spark_unstructured-spark-master-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jobs/hadoop-aws-3.3.4.jar,/opt/spark/jobs/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /opt/spark/jobs/main.py
```

### Testing Streaming

1. **Start the streaming job** (using one of the commands above)

2. **Add new files while it's running** (in another terminal):
```bash
# Add a new text file
cp input/input_text/sample.txt jobs/input/input_text/new_job_$(date +%s).txt

# Add a new JSON file
cp input/input_json/sample.json jobs/input/input_json/new_job_$(date +%s).json
```

3. **Check output**:
```bash
# Local mode
ls -la jobs/output/

# S3 mode
aws s3 ls s3://your-bucket/data/spark_unstructured/ --recursive
```

### UDF Functions by Format

| Format | Extraction Method | Key Challenges | Solution |
|--------|------------------|----------------|----------|
| **Text** | Regex patterns | Inconsistent formatting | Multiple regex patterns with fallbacks |
| **JSON** | Direct parsing | Nested structures | Schema-based extraction |
| **CSV** | Built-in reader | Column mapping | Predefined schema with type casting |
| **PDF** | PyMuPDF | Text layout variations | Text block extraction + regex |
| **Image** | Tesseract OCR | Low quality, skew | PIL preprocessing (contrast, sharpening) |

## 📸 Screenshots

### AWS S3 Bucket
<img width="1415" height="646" alt="image" src="https://github.com/user-attachments/assets/7c61e7ba-ba48-4522-9753-ddfbfd13f02a" />


### Glue Table
<img width="1139" height="454" alt="image" src="https://github.com/user-attachments/assets/70737869-fd2f-4305-9570-b4d9939234cd" />

<img width="1133" height="638" alt="image" src="https://github.com/user-attachments/assets/a2725282-8fd3-45ad-b0e0-55882c8176f6" />


### AWS Athena Query
<img width="1428" height="625" alt="image" src="https://github.com/user-attachments/assets/5e8bc650-f43f-46ca-96b5-45431ea171e3" />

### Output Table
<img width="1010" height="560" alt="image" src="https://github.com/user-attachments/assets/c742d9f8-3de9-45b4-9ef4-78eeb6975227" />
<img width="1008" height="563" alt="image" src="https://github.com/user-attachments/assets/2c7a0547-7eab-464b-b322-7323519e1a82" />


## 🎯 Use Cases

- **HR Analytics**: Process job posting data for market analysis
- **Recruitment Intelligence**: Extract and analyze hiring trends
- **Salary Benchmarking**: Compare compensation across positions
- **Skills Gap Analysis**: Identify required vs. available skills
- **Real-time Job Boards**: Update job databases in real-time

