import re
from datetime import datetime
import fitz  # PyMuPDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import io
from PIL import Image, ImageEnhance, ImageFilter
import pytesseract
import cv2
import numpy as np


def extract_file_name(file_content):
    """Extract filename from File Name field or generate from position"""
    file_content = file_content.strip()
    
    # Try to extract from "File Name:" field first
    file_name_match = re.search(r'File Name:\s*(.+?)(?:\n|\.txt|\.pdf|\.jpeg|\.jpg|\.png|$)', file_content, re.IGNORECASE)
    if file_name_match:
        filename = file_name_match.group(1).strip()
        # Remove file extension if present
        filename = re.sub(r'\.(txt|pdf|jpeg|jpg|png)$', '', filename, flags=re.IGNORECASE)
        return filename
    
    # Fallback: Extract from position + class code
    first_line = file_content.split('\n')[0] if '\n' in file_content else file_content
    position_part = re.sub(r'[\s\-]+', '_', first_line.lower())
    position_part = re.sub(r'_(senior|level|junior|mid|entry)_?', '_', position_part)
    position_part = re.sub(r'_+', '_', position_part).strip('_')
    
    class_code_match = re.search(r'(?:Class Code|Classcode):\s*(\d+)', file_content, re.IGNORECASE)
    class_code = class_code_match.group(1) if class_code_match else 'unknown'
    
    return f"{position_part}_{class_code}"

def extract_position(file_content):
    """Extract job position - handles multiple formats"""
    file_content = file_content.strip()
    
    # Try to find position after common keywords
    patterns = [
        r'(?:Position|Job Title|Role):\s*(.+?)(?:\n|$)',  # Explicit label
        r'^([A-Z][A-Za-z\s&-]+(?:Engineer|Manager|Analyst|Developer|Architect|Designer|Scientist|Administrator|Coordinator))',  # Job title pattern at start
    ]
    
    for pattern in patterns:
        match = re.search(pattern, file_content, re.IGNORECASE | re.MULTILINE)
        if match:
            position = match.group(1).strip()
            # Clean up common OCR artifacts
            position = re.sub(r'\s+', ' ', position)  # normalize whitespace
            position = re.sub(r'[^\w\s&-]', '', position)  # remove special chars except &-
            return position
    
    # Fallback: get first non-empty line that looks like a job title
    lines = file_content.split('\n')
    for line in lines[:10]:  # Check first 10 lines
        line = line.strip()
        # Check if line looks like a job title (has common job keywords)
        if (len(line) > 5 and len(line) < 100 and 
            any(keyword in line.lower() for keyword in 
                ['engineer', 'manager', 'analyst', 'developer', 'architect', 
                 'designer', 'scientist', 'administrator', 'coordinator', 
                 'specialist', 'director', 'lead', 'senior', 'junior'])):
            # Clean up
            line = re.sub(r'\s+', ' ', line)
            line = re.sub(r'[^\w\s&-]', '', line)
            return line
    
    # Last resort: return first line
    first_line = lines[0] if lines else "Position Not Found"
    return first_line.strip()[:100]

def extract_class_code(file_content):
    """Extract class code from Class Code or Classcode field"""
    match = re.search(r'(?:Class Code|Classcode):\s*(\d+)', file_content, re.IGNORECASE)
    return match.group(1) if match else None

def extract_start_date(file_content):
    """Extract start date from Start Date field"""
    # Try "Start Date: February 10, 2025" format
    match = re.search(r'Start Date:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})', file_content, re.IGNORECASE)
    if match:
        try:
            return datetime.strptime(match.group(1), '%B %d, %Y').date()
        except:
            pass
    
    # Try MM/DD/YYYY format
    match = re.search(r'Start Date:\s*(\d{2}/\d{2}/\d{4})', file_content, re.IGNORECASE)
    if match:
        try:
            return datetime.strptime(match.group(1), '%m/%d/%Y').date()
        except:
            pass
    
    # Fallback: try "Posted:" pattern
    match = re.search(r'Posted:\s*(\d{2}/\d{2}/\d{4})', file_content)
    if match:
        try:
            return datetime.strptime(match.group(1), '%m/%d/%Y').date()
        except:
            return None
    return None

def extract_salary(file_content):
    """Extract salary range - handles both $ and non-$ formats"""
    match = re.search(r'Salary Range:\s*\$?([\d,]+)\s*-\s*\$?([\d,]+)', file_content, re.IGNORECASE)
    if match:
        return {
            "salary_start": float(match.group(1).replace(',', '')),
            "salary_end": float(match.group(2).replace(',', ''))
        }
    return {"salary_start": None, "salary_end": None}

def extract_requirements(file_content):
    """Extract requirements - handles both old and new format"""
    # Try new format: "Requirements" section
    match = re.search(r'Requirements(.*?)(?:Benefits|Selection|Key Responsibilities|$)', 
                     file_content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback: old format
    match = re.search(r'REQUIRED QUALIFICATIONS:(.*?)(?:PREFERRED|TECHNICAL|$)', 
                     file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_benefits(file_content):
    """Extract benefits - handles both formats"""
    # Try new format: "Benefits & Perks"
    match = re.search(r'Benefits\s*&\s*Perks(.*?)(?:Selection|Application|$)', 
                     file_content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback: old format
    match = re.search(r'BENEFITS:(.*?)(?:COMPANY|$)', file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_duties(file_content):
    """Extract duties/responsibilities"""
    # Try new format: "Key Responsibilities"
    match = re.search(r'Key Responsibilities(.*?)(?:Requirements|Benefits|$)', 
                     file_content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback: old format
    match = re.search(r'ESSENTIAL DUTIES AND RESPONSIBILITIES:(.*?)(?:REQUIRED|$)', 
                     file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_selection_criteria(file_content):
    """Extract selection process/criteria"""
    # Try new format: "Selection Process"
    match = re.search(r'Selection Process(.*?)(?:Application Location|$)', 
                     file_content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()[:500]
    
    # Fallback: old format
    match = re.search(r'APPLICATION INSTRUCTIONS:(.*?)(?:CONTACT|$)', 
                     file_content, re.DOTALL)
    return match.group(1).strip()[:500] if match else None

def extract_experience_length(file_content):
    """Extract years of experience"""
    match = re.search(r'(\d+)\+?\s*years?\s+of.*?experience', file_content, re.IGNORECASE)
    return match.group(1) + ' years' if match else None

def extract_education_length(file_content):
    """Extract education level"""
    # ✅ Capture the entire phrase "Bachelor's degree", "Master's degree", or "PhD degree"
    match = re.search(r"(Bachelor's\s+degree|Master's\s+degree|PhD\s+degree)", file_content, re.IGNORECASE)
    if match:
        # Return exactly what was matched
        return match.group(1)
    return None

def extract_application_location(file_content):
    """Extract application location"""
    # Try new format: "Application Location:"
    match = re.search(r'Application Location:\s*(.+?)(?:\n|$)', file_content, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback: old format with Email
    match = re.search(r'Email:\s*(.+)', file_content)
    return match.group(1).strip() if match else None

def extract_pdf_text(pdf_content):
    """
    Extract text content from PDF binary data
    This is a placeholder - actual PDF extraction happens before this UDF
    """
    try:
        # This function processes already extracted text
        if pdf_content:
            return pdf_content
        return ""
    except Exception as e:
        print(f"Error extracting PDF text: {str(e)}")
        return ""

def extract_file_name_from_pdf(file_content):
    """Extract file name from PDF content"""
    try:
        # Try to find job title or position at the top
        lines = file_content.split('\n')
        for line in lines[:5]:  # Check first 5 lines
            line = line.strip()
            if len(line) > 5 and len(line) < 100:
                # Clean up the name
                name = re.sub(r'[^\w\s-]', '', line)
                return name.strip().upper()
        return "UNKNOWN_PDF"
    except:
        return "UNKNOWN_PDF"

def extract_position_from_pdf(file_content):
    """Extract position from PDF content"""
    try:
        # Look for position patterns
        patterns = [
            r'(?:Position|Job Title|Role):\s*(.+?)(?:\n|$)',
            r'^(.+?(?:Engineer|Manager|Analyst|Developer|Architect|Designer|Scientist).*?)(?:\n|Job)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, file_content, re.IGNORECASE | re.MULTILINE)
            if match:
                return match.group(1).strip()
        
        # Fallback: get first meaningful line
        lines = file_content.split('\n')
        for line in lines[:10]:
            if len(line.strip()) > 10:
                return line.strip()[:100]
        
        return "Position Not Found"
    except:
        return "Position Not Found"

def extract_class_code_from_pdf(file_content):
    """Extract class code from PDF content"""
    try:
        patterns = [
            r'(?:Class Code|Job Code|Position Code|ID):\s*(\d+)',
            r'(?:Code|ID)\s*[:#]?\s*(\d{4,6})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, file_content, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return "0000"
    except:
        return "0000"
    

def extract_text_from_pdf_bytes(pdf_bytes):
    """
    Extract text from PDF binary content using PyMuPDF
    """
    try:
        if pdf_bytes is None:
            return ""
        
        # Convert bytes to file-like object
        pdf_file = io.BytesIO(pdf_bytes)
        
        text_content = ""
        with fitz.open(stream=pdf_file, filetype="pdf") as doc:
            for page in doc:
                text_content += page.get_text() + "\n"
        
        return text_content
    except Exception as e:
        print(f"Error extracting PDF: {str(e)}")
        return ""

def preprocess_image_for_ocr(image):
    """
    Preprocess image to improve OCR accuracy
    """
    # Convert PIL Image to numpy array
    img_array = np.array(image)
    
    # Convert to grayscale
    if len(img_array.shape) == 3:
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
    else:
        gray = img_array
    
    # Apply thresholding to get black text on white background
    _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    # Denoise
    denoised = cv2.fastNlMeansDenoising(thresh)
    
    # Convert back to PIL Image
    return Image.fromarray(denoised)

def extract_text_from_image_bytes(image_bytes):
    """
    Extract text from image binary content using OCR (Tesseract) with preprocessing
    """
    try:
        if image_bytes is None:
            return ""
        
        # Convert bytes to PIL Image
        image_file = io.BytesIO(image_bytes)
        image = Image.open(image_file)
        
        # Convert to RGB if needed
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        # Resize if image is too small (improves OCR)
        width, height = image.size
        if width < 1000:
            scale = 2000 / width
            new_size = (int(width * scale), int(height * scale))
            image = image.resize(new_size, Image.Resampling.LANCZOS)
        
        # Enhance image quality
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2.0)  # Increase contrast
        
        enhancer = ImageEnhance.Sharpness(image)
        image = enhancer.enhance(2.0)  # Increase sharpness
        
        # Preprocess image
        image = preprocess_image_for_ocr(image)
        
        # Use OCR with custom configuration for better accuracy
        custom_config = r'--oem 3 --psm 6 -c preserve_interword_spaces=1'
        text_content = pytesseract.image_to_string(image, config=custom_config)
        
        return text_content
    except Exception as e:
        print(f"Error extracting image text: {str(e)}")
        return ""

# Create UDF for image text extraction
extract_image_text_udf = udf(extract_text_from_image_bytes, StringType())

# Create UDF for PDF text extraction
extract_pdf_text_udf = udf(extract_text_from_pdf_bytes, StringType())