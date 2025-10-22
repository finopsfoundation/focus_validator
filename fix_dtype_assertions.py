#!/usr/bin/env python3
"""
Script to fix dtype assertions in test files to use Polars types instead of string comparisons.
"""

import os
import re
import sys
from pathlib import Path

def fix_dtype_assertions(file_path):
    """Fix dtype assertions in a test file to use Polars types."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Fix assertions comparing dtypes to strings
        replacements = [
            # Basic type comparisons
            (r"\.assertEqual\(([^,]+)\.dtype,\s*'float64'\)", r".assertEqual(\1.dtype, pl.Float64)"),
            (r"\.assertEqual\(([^,]+)\.dtype,\s*'int64'\)", r".assertEqual(\1.dtype, pl.Int64)"),
            (r"\.assertEqual\(([^,]+)\.dtype,\s*'string'\)", r".assertEqual(\1.dtype, pl.Utf8)"),
            
            # Datetime comparisons
            (r"\.assertEqual\(str\(([^)]+)\.dtype\),\s*pl\.Datetime\([^)]+\)\)", r".assertEqual(\1.dtype, pl.Datetime('us', 'UTC'))"),
            
            # Dtype.name comparisons 
            (r"\.assertEqual\(([^,]+)\.dtype\.name,\s*'string'\)", r".assertEqual(\1.dtype, pl.Utf8)"),
            (r"\.assertEqual\(([^,]+)\.dtype\.name,\s*'float64'\)", r".assertEqual(\1.dtype, pl.Float64)"),
            (r"\.assertEqual\(([^,]+)\.dtype\.name,\s*'int64'\)", r".assertEqual(\1.dtype, pl.Int64)"),
            
            # AssertNotEqual for dtype.name
            (r"\.assertNotEqual\(([^,]+)\.dtype\.name,\s*'string'\)", r".assertNotEqual(\1.dtype, pl.Utf8)"),
            (r"\.assertNotEqual\(([^,]+)\.dtype\.name,\s*'float64'\)", r".assertNotEqual(\1.dtype, pl.Float64)"),
            (r"\.assertNotEqual\(([^,]+)\.dtype\.name,\s*'int64'\)", r".assertNotEqual(\1.dtype, pl.Int64)"),
            
            # String comparisons with startswith for datetime
            (r"\.assertTrue\(str\(([^)]+)\.dtype\)\.startswith\('datetime64'\)\)", r".assertIsInstance(\1.dtype, pl.Datetime)"),
            
            # is_null() method fix
            (r"\.isnull\(\)", r".is_null()"),
            
            # DataFrame indexing syntax
            (r"\.iloc\[([^\]]+)\]", r"[\1]"),
        ]
        
        # Apply replacements
        for pattern, replacement in replacements:
            content = re.sub(pattern, replacement, content)
        
        # Check if polars import is needed and missing
        if 'pl.' in content and 'import polars as pl' not in content:
            # Add polars import after other imports
            import_match = re.search(r'(import [^\n]+\n)+', content)
            if import_match:
                import_section = import_match.group(0)
                new_import_section = import_section + 'import polars as pl\n'
                content = content.replace(import_section, new_import_section, 1)
            else:
                # Add at the beginning if no imports found
                content = 'import polars as pl\n' + content
        
        # Only write if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Fixed dtype assertions in {file_path}")
            return True
        else:
            print(f"- No dtype assertion changes needed in {file_path}")
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def main():
    """Main function to fix dtype assertions in test files."""
    
    if len(sys.argv) > 1:
        # Process specific files
        files_to_process = sys.argv[1:]
    else:
        # Process all test files
        test_dir = Path("tests")
        files_to_process = list(test_dir.glob("**/*.py"))
    
    print("Fixing dtype assertions in test files...")
    
    fixed_count = 0
    total_count = 0
    
    for file_path in files_to_process:
        total_count += 1
        if fix_dtype_assertions(file_path):
            fixed_count += 1
    
    print(f"\nProcessed {total_count} files, fixed {fixed_count} files")

if __name__ == "__main__":
    main()