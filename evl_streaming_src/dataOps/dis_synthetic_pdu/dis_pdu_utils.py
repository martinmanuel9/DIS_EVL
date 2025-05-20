#!/usr/bin/python

"""
DIS PDU Verification Utility

This script loads generated PDU datasets and performs verification and validation
to ensure they conform to IEEE-1278.1 standards and contain expected content.
"""

import os
import sys
import base64
import pickle
import argparse
import pandas as pd
import numpy as np
from io import BytesIO

# Import Open DIS classes
from opendis.dis7 import *
from opendis.DataInputStream import DataInputStream
from opendis.DataOutputStream import DataOutputStream

def load_dataset(file_path):
    """Load a PDU dataset from a pickle or parquet file"""
    if file_path.endswith('.pkl'):
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

def extract_binary_pdu(row):
    """Extract binary PDU data from a DataFrame row or dictionary"""
    if isinstance(row, pd.Series):
        if 'pdu_binary' in row:
            # For parquet files, binary data is base64 encoded
            if isinstance(row['pdu_binary'], str):
                return base64.b64decode(row['pdu_binary'])
            # For pickle files or already decoded, return as is
            return row['pdu_binary']
    elif isinstance(row, dict):
        if 'pdu_binary' in row:
            if isinstance(row['pdu_binary'], str):
                return base64.b64decode(row['pdu_binary'])
            return row['pdu_binary']
    
    return None

def deserialize_pdu(binary_data):
    """Deserialize binary PDU data to a PDU object"""
    memory_stream = BytesIO(binary_data)
    input_stream = DataInputStream(memory_stream)
    
    # Get PDU type from the 3rd byte
    if len(binary_data) < 3:
        return None
        
    pdu_type = binary_data[2]
    
    # Create appropriate PDU object based on type
    pdu = None
    if pdu_type == 1:  # EntityStatePdu
        pdu = EntityStatePdu()
    elif pdu_type == 2:  # FirePdu
        pdu = FirePdu()
    elif pdu_type == 3:  # DetonationPdu
        pdu = DetonationPdu()
    else:
        # Default to generic PDU
        pdu = Pdu()
    
    # Parse the data
    try:
        pdu.parse(input_stream)
        return pdu
    except Exception as e:
        print(f"Error deserializing PDU: {e}")
        return None

def verify_pdu_fields(row, pdu):
    """Verify that PDU object fields match the metadata in the row"""
    issues = []
    
    # Check PDU type
    pdu_type_map = {
        'EntityStatePdu': 1,
        'FirePdu': 2,
        'DetonationPdu': 3
    }
    
    expected_pdu_type = pdu_type_map.get(row.get('pdu_type'))
    if expected_pdu_type is not None and pdu.pduType != expected_pdu_type:
        issues.append(f"PDU type mismatch: expected {expected_pdu_type}, got {pdu.pduType}")
    
    # Check entity ID
    if hasattr(pdu, 'entityID') and 'entity_id' in row:
        if pdu.entityID.entityID != row['entity_id']:
            issues.append(f"Entity ID mismatch: expected {row['entity_id']}, got {pdu.entityID.entityID}")
    
    # Check force ID (for EntityStatePdu)
    if hasattr(pdu, 'forceId') and 'force_id' in row:
        if pdu.forceId != row['force_id']:
            issues.append(f"Force ID mismatch: expected {row['force_id']}, got {pdu.forceId}")
    
    # For EntityStatePdu, check position
    if pdu.pduType == 1 and hasattr(pdu, 'entityLocation'):
        x_match = True
        y_match = True
        z_match = True
        
        if 'position_x' in row and abs(pdu.entityLocation.x - row['position_x']) > 0.001:
            x_match = False
            issues.append(f"Position X mismatch: expected {row['position_x']}, got {pdu.entityLocation.x}")
        
        if 'position_y' in row and abs(pdu.entityLocation.y - row['position_y']) > 0.001:
            y_match = False
            issues.append(f"Position Y mismatch: expected {row['position_y']}, got {pdu.entityLocation.y}")
        
        if 'position_z' in row and abs(pdu.entityLocation.z - row['position_z']) > 0.001:
            z_match = False
            issues.append(f"Position Z mismatch: expected {row['position_z']}, got {pdu.entityLocation.z}")
            
    return issues

def verify_dataset(dataset):
    """Verify all PDUs in a dataset"""
    print(f"Verifying {len(dataset)} PDUs...")
    
    error_count = 0
    success_count = 0
    pdu_type_counts = {}
    
    # For DataFrame
    if isinstance(dataset, pd.DataFrame):
        for idx, row in dataset.iterrows():
            binary_data = extract_binary_pdu(row)
            if binary_data is None:
                error_count += 1
                print(f"Row {idx}: No binary PDU data found")
                continue
                
            pdu = deserialize_pdu(binary_data)
            if pdu is None:
                error_count += 1
                print(f"Row {idx}: Failed to deserialize PDU")
                continue
                
            issues = verify_pdu_fields(row, pdu)
            if issues:
                error_count += 1
                print(f"Row {idx}: {', '.join(issues)}")
            else:
                success_count += 1
                
            # Track PDU types
            pdu_type = pdu.pduType
            pdu_type_counts[pdu_type] = pdu_type_counts.get(pdu_type, 0) + 1
    
    # For list (pickle format)
    elif isinstance(dataset, list):
        for idx, row in enumerate(dataset):
            binary_data = extract_binary_pdu(row)
            if binary_data is None:
                error_count += 1
                print(f"Row {idx}: No binary PDU data found")
                continue
                
            pdu = deserialize_pdu(binary_data)
            if pdu is None:
                error_count += 1
                print(f"Row {idx}: Failed to deserialize PDU")
                continue
                
            issues = verify_pdu_fields(row, pdu)
            if issues:
                error_count += 1
                print(f"Row {idx}: {', '.join(issues)}")
            else:
                success_count += 1
                
            # Track PDU types
            pdu_type = pdu.pduType
            pdu_type_counts[pdu_type] = pdu_type_counts.get(pdu_type, 0) + 1
    
    # Print summary
    print("\nVerification Summary:")
    print(f"Total PDUs: {len(dataset)}")
    print(f"Successfully verified: {success_count} ({success_count/len(dataset)*100:.2f}%)")
    print(f"Errors: {error_count} ({error_count/len(dataset)*100:.2f}%)")
    
    print("\nPDU Type Distribution:")
    pdu_type_names = {
        1: "EntityStatePdu",
        2: "FirePdu",
        3: "DetonationPdu"
    }
    for pdu_type, count in sorted(pdu_type_counts.items()):
        pdu_name = pdu_type_names.get(pdu_type, f"Unknown Type {pdu_type}")
        print(f"  {pdu_name}: {count} ({count/len(dataset)*100:.2f}%)")
    
    return success_count, error_count

def main():
    parser = argparse.ArgumentParser(description='Verify DIS PDU datasets')
    parser.add_argument('file', help='Path to PDU dataset file (.pkl or .parquet)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed errors')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: File {args.file} not found")
        return 1
    
    try:
        dataset = load_dataset(args.file)
        verify_dataset(dataset)
        return 0
    except Exception as e:
        print(f"Error processing file: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())