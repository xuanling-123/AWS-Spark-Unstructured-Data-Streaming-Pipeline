from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, BinaryType, TimestampType, LongType
from pyspark.sql.functions import udf, col, input_file_name, regexp_extract, when
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf

from config.config import configuration
from udf_utils import * 


def define_udfs():
    udf_dict = {
        'extract_filename_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_class_code_udf': udf(extract_class_code, StringType()),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField("salary_start", DoubleType(), True),
            StructField("salary_end", DoubleType(), True)
        ])),
        'extract_requirements_udf': udf(extract_requirements, StringType()),
        'extract_benefits_udf': udf(extract_benefits, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_criteria_udf': udf(extract_selection_criteria, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_education_length_udf': udf(extract_education_length, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType())
    }
    return udf_dict

data_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("classcode", StringType(), True),
    StructField("salary_start", IntegerType(), True),
    StructField("salary_end", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("req", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("duties", StringType(), True),
    StructField("selection", StringType(), True),
    StructField("experience_length", StringType(), True),
    StructField("job_type", StringType(), True),
    StructField("education_length", StringType(), True),
    StructField("school_type", StringType(), True),
    StructField("application_location", StringType(), True)
])


if __name__ == "__main__":
    USE_S3 = configuration['AWS_ACCESS_KEY'] != '' and configuration['AWS_SECRET_KEY'] != ''
    
    spark_builder = (SparkSession.builder
                .appName("AWS_Spark_Unstructured")
                .master("local[*]")
            )
    
    if USE_S3:
        print("AWS S3 MODE - Configuring Spark for S3 access")
        spark_builder = spark_builder \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.access.key", configuration['AWS_ACCESS_KEY']) \
                .config("spark.hadoop.fs.s3a.secret.key", configuration['AWS_SECRET_KEY']) \
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
                .config("spark.hadoop.fs.s3a.fast.upload", "true") \
                .config("spark.hadoop.fs.s3a.path.style.access", "false")

    else:
        print("LOCAL MODE - Using local filesystem")
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # text_input_dir = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/input/input_text'
    # json_input_dir = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/input/input_json'
    # csv_input_dir = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/jobs/input/input_csv'
    # pdf_input_dir = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/jobs/input/input_pdf'
    # image_input_dir = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/jobs/input/input_image'

    text_input_dir = 'file:///opt/spark/jobs/input/input_text'
    json_input_dir = 'file:///opt/spark/jobs/input/input_json'
    csv_input_dir = 'file:///opt/spark/jobs/input/input_csv' 
    pdf_input_dir = 'file:///opt/spark/jobs/input/input_pdf'
    image_input_dir = 'file:///opt/spark/jobs/input/input_image'



    if USE_S3:
        bucket = configuration['S3_BUCKET']
        output_path = f"s3a://{bucket}/data/spark_unstructured/"
        checkpoint_path = f"s3a://{bucket}/checkpoints/"

    else:
        # output_path = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/data/spark_unstructured/'
        # checkpoint_path = 'file:///Users/poonx/Documents/repo/Personal/AWS_SPARK_UNSTRUCTURED/checkpoints/'
        output_path = 'file:///opt/spark/jobs/output/'
        checkpoint_path = 'file:///opt/spark/jobs/checkpoints/'

    udfs = define_udfs()

    print("\n" + "="*80)
    print("SETTING UP STREAMING SOURCES")
    print("="*80 + "\n")

    # =================================================================
    # STREAM 1: TEXT FILES
    # =================================================================
    print("📝 Setting up TEXT stream...")
    
    raw_text_stream = (spark.readStream
                      .format("text")
                      .option('wholetext', 'true')
                      .option("maxFilesPerTrigger", 1) 
                      .load(text_input_dir)
                     )

    processed_text_df = raw_text_stream.select(
        udfs['extract_filename_udf'](col("value")).alias("file_name"),
        udfs['extract_position_udf'](col("value")).alias("position"),
        udfs['extract_class_code_udf'](col("value")).alias("classcode"),
        udfs['extract_salary_udf'](col("value")).alias("salary"),
        udfs['extract_start_date_udf'](col("value")).alias("start_date"),
        udfs['extract_requirements_udf'](col("value")).alias("req"),
        udfs['extract_benefits_udf'](col("value")).alias("benefits"),
        udfs['extract_duties_udf'](col("value")).alias("duties"),
        udfs['extract_selection_criteria_udf'](col("value")).alias("selection"),
        udfs['extract_experience_length_udf'](col("value")).alias("experience_length"),
        udfs['extract_education_length_udf'](col("value")).alias("education_length"),
        udfs['extract_application_location_udf'](col("value")).alias("application_location")
    ).select(
        col("file_name"),
        col("position"),
        col("classcode"),
        col("salary.salary_start").alias("salary_start"),
        col("salary.salary_end").alias("salary_end"),
        col("start_date"),
        col("req"),
        col("benefits"),
        col("duties"),
        col("selection"),
        col("experience_length"),
        col("education_length"),
        col("application_location")
    )
    
    # =================================================================
    # STREAM 2: JSON FILES
    # =================================================================
    print("📋 Setting up JSON stream...")
    raw_json_stream = (spark.readStream
                       .format("json")
                       .schema(data_schema)
                       .option('multiLine', 'true')
                       .option("maxFilesPerTrigger", 1)
                       .load(json_input_dir)
                      )

    
    processed_json_df = raw_json_stream.select(
                            col("file_name"),
                            col("position"),
                            col("classcode"),
                            col("salary_start").cast(DoubleType()),
                            col("salary_end").cast(DoubleType()),
                            col("start_date").cast(DateType()),
                            col("req"),
                            col("notes").alias("benefits"),
                            col("duties"),
                            col("selection"),
                            col("experience_length"),
                            col("education_length"),
                            col("application_location")
                        )
    
    # =================================================================
    # STREAM 3: CSV FILES
    # =================================================================
    print("📊 Setting up CSV stream...")
    raw_csv_stream = (spark.readStream
                      .format("csv")
                      .schema(data_schema)
                      .option("header", "true")
                      .option("inferSchema", "false")
                      .option("maxFilesPerTrigger", 1)
                      .load(csv_input_dir)
                     )

    processed_csv_df = raw_csv_stream.select(
                            col("file_name"),
                            col("position"),
                            col("classcode"),
                            col("salary_start").cast(DoubleType()),
                            col("salary_end").cast(DoubleType()),
                            col("start_date").cast(DateType()),
                            col("req"),
                            col("notes").alias("benefits"),
                            col("duties"),
                            col("selection"),
                            col("experience_length"),
                            col("education_length"),
                            col("application_location")
                        )
    
    # =================================================================
    # STREAM 4: PDF FILES
    # =================================================================
    print("📄 Setting up PDF stream...")

    binary_file_schema = StructType([
        StructField("path", StringType(), False),          
        StructField("modificationTime", TimestampType(), False), 
        StructField("length", LongType(), False),          
        StructField("content", BinaryType(), True)
    ])

    # Read PDF files as binary
    raw_pdf_stream = (spark.readStream
                      .format("binaryFile")
                      .schema(binary_file_schema)
                      .option("pathGlobFilter", "*.pdf")
                      .option("maxFilesPerTrigger", 1)
                      .load(pdf_input_dir)
                     )
    
    # Extract text from PDF binary content
    pdf_with_text = raw_pdf_stream.withColumn(
        "extracted_text", 
        extract_pdf_text_udf(col("content"))
    )
    
    # Extract structured data from PDF text
    processed_pdf_df = pdf_with_text.select(
        udfs['extract_filename_udf'](col("extracted_text")).alias("file_name"),
        udfs['extract_position_udf'](col("extracted_text")).alias("position"),
        udfs['extract_class_code_udf'](col("extracted_text")).alias("classcode"),
        udfs['extract_salary_udf'](col("extracted_text")).alias("salary"),
        udfs['extract_start_date_udf'](col("extracted_text")).alias("start_date"),
        udfs['extract_requirements_udf'](col("extracted_text")).alias("req"),
        udfs['extract_benefits_udf'](col("extracted_text")).alias("benefits"),
        udfs['extract_duties_udf'](col("extracted_text")).alias("duties"),
        udfs['extract_selection_criteria_udf'](col("extracted_text")).alias("selection"),
        udfs['extract_experience_length_udf'](col("extracted_text")).alias("experience_length"),
        udfs['extract_education_length_udf'](col("extracted_text")).alias("education_length"),
        udfs['extract_application_location_udf'](col("extracted_text")).alias("application_location")
    ).select(
        col("file_name"),
        col("position"),
        col("classcode"),
        col("salary.salary_start").alias("salary_start"),
        col("salary.salary_end").alias("salary_end"),
        col("start_date"),
        col("req"),
        col("benefits"),
        col("duties"),
        col("selection"),
        col("experience_length"),
        col("education_length"),
        col("application_location")
    )

    # =================================================================
    # STREAM 5: IMAGE FILES
    # =================================================================
    print("🖼️  Setting up IMAGE stream...")

    # Read image files as binary (supports jpg, jpeg, png, tiff, bmp)
    raw_image_stream = (spark.readStream
                        .format("binaryFile")
                        .schema(binary_file_schema)
                        .option("pathGlobFilter", "*.{jpg,jpeg,png,tiff,bmp}")
                        .option("maxFilesPerTrigger", 1)
                        .load(image_input_dir)
                       )
    
    
    # Extract text from images using OCR
    image_with_text = raw_image_stream.withColumn(
        "extracted_text", 
        extract_image_text_udf(col("content"))
    )

    # Extract structured data from image text
    processed_image_df = image_with_text.select(
        udfs['extract_filename_udf'](col("extracted_text")).alias("file_name"),
        udfs['extract_position_udf'](col("extracted_text")).alias("position"),
        udfs['extract_class_code_udf'](col("extracted_text")).alias("classcode"),
        udfs['extract_salary_udf'](col("extracted_text")).alias("salary"),
        udfs['extract_start_date_udf'](col("extracted_text")).alias("start_date"),
        udfs['extract_requirements_udf'](col("extracted_text")).alias("req"),
        udfs['extract_benefits_udf'](col("extracted_text")).alias("benefits"),
        udfs['extract_duties_udf'](col("extracted_text")).alias("duties"),
        udfs['extract_selection_criteria_udf'](col("extracted_text")).alias("selection"),
        udfs['extract_experience_length_udf'](col("extracted_text")).alias("experience_length"),
        udfs['extract_education_length_udf'](col("extracted_text")).alias("education_length"),
        udfs['extract_application_location_udf'](col("extracted_text")).alias("application_location")
    ).select(
        col("file_name"),
        col("position"),
        col("classcode"),
        col("salary.salary_start").alias("salary_start"),
        col("salary.salary_end").alias("salary_end"),
        col("start_date"),
        col("req"),
        col("benefits"),
        col("duties"),
        col("selection"),
        col("experience_length"),
        col("education_length"),
        col("application_location")
    )
    
    # =================================================================
    # UNION ALL STREAMS
    # =================================================================
    print("\n🔗 Combining all streams...")
    union_df = (processed_text_df
                .union(processed_json_df)
                .union(processed_csv_df)
                .union(processed_pdf_df)
                .union(processed_image_df))

    # =================================================================
    # START STREAMING QUERY
    # =================================================================
    print("\n" + "="*80)
    print("STARTING STREAMING QUERY")
    print("="*80 + "\n")
    
    try:
        query = (union_df.writeStream
                .outputMode("append") 
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime='10 seconds')  # Process every 10 seconds
                .start()
        )
        
        print(f"✅ Streaming query started")
        print(f"   Output path: {output_path}")
        print(f"   Checkpoint path: {checkpoint_path}")
        print(f"   Processing interval: 10 seconds")
        print(f"\n⏳ Waiting for data... (Press Ctrl+C to stop)\n")
        
        # Wait for the streaming query to finish
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping streaming query...")
        query.stop()
        print("✅ Streaming query stopped")
    except Exception as e:
        print(f"❌ Error in streaming: {str(e)}")
    finally:
        spark.stop()
        print("\n✅ Spark session stopped\n")
