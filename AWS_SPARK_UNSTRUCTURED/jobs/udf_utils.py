import re
from datetime import datetime

def extract_file_name(file_content):
    """Extract filename in format: position_jobid (e.g., software_engineer_4567)"""
    file_content = file_content.strip()
    
    # Extract position (first line)
    first_line = file_content.split('\n')[0] if '\n' in file_content else file_content
    # Convert to lowercase, replace spaces and hyphens with underscores
    position_part = re.sub(r'[\s\-]+', '_', first_line.lower())
    # Remove "senior", "level", and other common suffixes
    position_part = re.sub(r'_(senior|level|junior|mid|entry)_?', '_', position_part)
    # Clean up multiple underscores
    position_part = re.sub(r'_+', '_', position_part).strip('_')
    
    # Extract Job ID
    job_id_match = re.search(r'Job ID:\s*(\d+)', file_content)
    job_id = job_id_match.group(1) if job_id_match else 'unknown'
    
    # Combine: position_jobid
    filename = f"{position_part}_{job_id}"
    
    return filename

def extract_position(file_content):
    """Extract job position - first line of the file"""
    file_content = file_content.strip()
    first_line = file_content.split('\n')[0] if '\n' in file_content else file_content
    return first_line.strip()

def extract_class_code(file_content):
    """Extract job ID"""
    match = re.search(r'Job ID:\s*(\d+)', file_content)
    return match.group(1) if match else None

def extract_start_date(file_content):
    """Extract posted date"""
    match = re.search(r'Posted:\s*(\d{2}/\d{2}/\d{4})', file_content)
    if match:
        try:
            return datetime.strptime(match.group(1), '%m/%d/%Y').date()
        except:
            return None
    return None

def extract_salary(file_content):
    """Extract salary range"""
    match = re.search(r'Salary Range:\s*\$?([\d,]+)\s*-\s*\$?([\d,]+)', file_content)
    if match:
        return {
            "salary_start": float(match.group(1).replace(',', '')),
            "salary_end": float(match.group(2).replace(',', ''))
        }
    return {"salary_start": None, "salary_end": None}

def extract_requirements(file_content):
    """Extract required qualifications"""
    match = re.search(r'REQUIRED QUALIFICATIONS:(.*?)(?:PREFERRED|TECHNICAL|$)', 
                     file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_benefits(file_content):
    """Extract benefits"""
    match = re.search(r'BENEFITS:(.*?)(?:COMPANY|$)', file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_duties(file_content):
    """Extract duties"""
    match = re.search(r'ESSENTIAL DUTIES AND RESPONSIBILITIES:(.*?)(?:REQUIRED|$)', 
                     file_content, re.DOTALL)
    return match.group(1).strip() if match else None

def extract_selection_criteria(file_content):
    """Extract application instructions"""
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
    """Extract contact email"""
    match = re.search(r'Email:\s*(.+)', file_content)
    return match.group(1).strip() if match else None