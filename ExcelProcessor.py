# -*- coding: utf-8 -*-
"""
Created on Thu Dec 12 19:07:49 2024

@author: swaro
"""

from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import re
import os
from typing import Dict, List, Union, Optional
import json


class ExcelProcessor:
    def __init__(self):
        self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        
        self.raw_dir = self.base_dir / "raw_files"
        self.output_dir = self.base_dir / "processed_files"
        
        self.raw_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        self.logger = self.setup_logging()

    def setup_logging(self):
        logger = logging.getLogger("excel_processor")
        logger.setLevel(logging.INFO)

        log_file = self.base_dir / "excel_processing.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def process_file(self, file_path: str):
        try:
            self.logger.info(f"Starting to process file: {file_path}")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_folder = self.output_dir / timestamp
            output_folder.mkdir(exist_ok=True)

            raw_file = self.persist_file(file_path, timestamp)
            self.logger.info(f"Raw file stored at: {raw_file}")

            processed_files = self.process_with_pandas(file_path, output_folder)
            
            metadata = {
                'original_file': file_path,
                'processing_time': timestamp,
                'processed_sheets': processed_files
            }
            
            metadata_file = output_folder / 'metadata.json'
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

            self.logger.info(f"Processing completed. Results in {output_folder}")
            return processed_files

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    def persist_file(self, file_path: str, timestamp: str):
        file_name = Path(file_path).name
        raw_file = self.raw_dir / f"{timestamp}_{file_name}"

        try:
            with open(file_path, 'rb') as source, open(raw_file, 'wb') as target:
                target.write(source.read())
            return str(raw_file)
        except Exception as e:
            self.logger.error(f"Error storing raw file: {str(e)}")
            raise

    def process_with_pandas(self, file_path, output_folder):
        try:
            excel_file = pd.ExcelFile(file_path)
            processed_files = {}

            for sheet_name in excel_file.sheet_names:
                try:
                    self.logger.info(f"Processing sheet: {sheet_name}")
                    
                    df = pd.read_excel(
                        excel_file,
                        sheet_name=sheet_name,
                        header=0
                    )

                    df = self.process_sheet(df, sheet_name)
                    
                    parquet_name = f"{sheet_name.lower().replace(' ', '_')}.parquet"
                    parquet_path = output_folder / parquet_name
                    df.to_parquet(parquet_path, index=False)
                    
                    processed_files[sheet_name] = str(parquet_path)
                    self.logger.info(f"Saved sheet {sheet_name} to {parquet_path}")

                except Exception as e:
                    self.logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                    continue

            return processed_files
            
        except Exception as e:
            self.logger.error(f"Error in processing: {str(e)}")
            raise

    def detect_header_structure(self, excel_file: pd.ExcelFile, sheet_name: str):
        try:
            sample = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=5)
            
            has_nested = isinstance(sample.columns, pd.MultiIndex)
            
            if has_nested:
                header_rows = list(range(len(sample.columns.levels)))
            else:
                header_rows = 0
                
                self.logger.info(f"Using row 0 as header with columns: {sample.columns.tolist()}")
            
            return {
                'has_nested': has_nested,
                'header_rows': header_rows
            }
        except Exception as e:
            self.logger.error(f"Error detecting header structure: {str(e)}")
            raise

    def process_sheet(self, df: pd.DataFrame, sheet_name: str):
        try:
            df = self.normalize_columns(df)
            self.logger.info(f"Normalized columns for sheet {sheet_name}")

            df = self.infer_types(df)
            self.logger.info(f"Inferred types for sheet {sheet_name}")

            self.validate_data(df)
            self.logger.info(f"Validated data for sheet {sheet_name}")

            return df
        except Exception as e:
            self.logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
            raise

    def normalize_columns(self, df: pd.DataFrame):
        def clean_name(name: str):
            name = str(name).lower().strip()
            
            name = name.replace('(mm/dd/yyyy)', 'date')
            name = name.replace('($)', 'usd')
            name = name.replace('(#)', 'number')
            name = name.replace('(type)', 'type')
            name = name.replace('(current)', 'current')
            name = name.replace('[current]', 'current')
            name = name.replace('&', 'and')
            name = name.replace('@', 'at')
            
            name = re.sub(r'[^a-z0-9_]', '_', name)
            name = re.sub(r'_+', '_', name)
            name = name.strip('_')
            
            if name[0].isdigit():
                name = f'col_{name}'
                
            return name
    
        if isinstance(df.columns, pd.MultiIndex):
            columns = ['_'.join(str(level).strip() for level in col if pd.notna(level))
                      for col in df.columns.values]
        else:
            columns = df.columns
    
        seen = set()
        clean_columns = []
        for col in columns:
            clean_col = clean_name(col)
            if clean_col in seen:
                i = 1
                while f"{clean_col}_{i}" in seen:
                    i += 1
                clean_col = f"{clean_col}_{i}"
            seen.add(clean_col)
            clean_columns.append(clean_col)
    
        df.columns = clean_columns
        return df

    def infer_types(self, df: pd.DataFrame):
        for column in df.columns:
            try:
                sample = df[column].dropna().head(1000)
                
                if len(sample) == 0:
                    continue

                if self.is_date(sample):
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    continue

                if self.is_numeric(sample):
                    if self.is_integer(sample):
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    else:
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    continue

                df[column] = df[column].astype(str)

            except Exception as e:
                self.logger.warning(f"Error inferring type for column {column}: {str(e)}")
                df[column] = df[column].astype(str)

        return df

    def is_date(self, series: pd.Series):
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-[A-Za-z]{3}-\d{4}'
        ]
        sample = series.astype(str).head(100)
        
        if any(sample.str.match(pat).any() for pat in date_patterns):
            try:
                pd.to_datetime(sample, errors='raise')
                return True
            except:
                return False
        return False

    def is_numeric(self, series: pd.Series):
        try:
            pd.to_numeric(series, errors='raise')
            return True
        except:
            return False

    def is_integer(self, series: pd.Series):
        numeric_series = pd.to_numeric(series, errors='coerce')
        return numeric_series.notna().all() and (numeric_series % 1 == 0).all()

    def validate_data(self, df: pd.DataFrame):
        validation_results = {
            'row_count': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'column_types': df.dtypes.astype(str).to_dict()
        }
        
        self.logger.info(f"Validation results: {json.dumps(validation_results, default=str)}")

        for column, null_count in validation_results['null_counts'].items():
            null_percentage = (null_count / validation_results['row_count']) * 100
            if null_percentage > 50:
                self.logger.warning(f"High null percentage ({null_percentage:.1f}%) in column: {column}")

if __name__ == "__main__":
    processor = ExcelProcessor()
    
    try:
        results = processor.process_file("C:/Users/swaro/Desktop/Keyes/financial_sample_sheet.xlsx") # Update the file path
        
        for sheet_name, parquet_path in results.items():
            df = pd.read_parquet(parquet_path)
            print(f"Columns: {df.columns.tolist()}")
            print(f"Row count: {len(df)}")
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")