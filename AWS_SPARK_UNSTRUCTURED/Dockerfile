FROM apache/spark:3.5.0

# Switch to root to install packages
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    libtesseract-dev \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies with specific versions
RUN pip install --no-cache-dir \
    python-dotenv \
    PyMuPDF \
    Pillow==10.1.0 \
    pytesseract==0.3.10 \
    opencv-python-headless==4.8.1.78 \
    numpy==1.24.3

# Switch back to spark user
USER spark

# Set working directory
WORKDIR /opt/spark/jobs