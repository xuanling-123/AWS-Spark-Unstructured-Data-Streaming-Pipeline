import os
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent

dotenv_path = project_root / '.env'
print(f"Loading environment variables from: {dotenv_path}")

load_dotenv(dotenv_path=dotenv_path)


configuration = {
    'AWS_ACCESS_KEY': os.getenv('AWS_ACCESS_KEY_ID', ''),
    'AWS_SECRET_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    'AWS_REGION': os.getenv('AWS_REGION', 'us-east-1'),
    'S3_BUCKET': os.getenv('S3_BUCKET', 'your-default-bucket'),
}

print("Configuration Loaded:", configuration)