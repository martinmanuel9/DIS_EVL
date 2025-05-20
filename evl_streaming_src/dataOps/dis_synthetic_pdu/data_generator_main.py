#!/usr/bin/python

"""
DIS PDU Data Generator

Main script to generate a comprehensive dataset of DIS PDUs for various combat scenarios.
This script calls all the individual scenario generators and produces labeled datasets in
both pickle and parquet formats.
"""

import os
import shutil
import time
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

# Import our scenario generators
from air_to_air_pdu_generator import AirToAirPDUGenerator
from air_to_surface_pdu_generator import AirToSurfacePDUGenerator
from surface_to_air_pdu_generator import SurfaceToAirPDUGenerator
from surface_to_surface_pdu_generator import SurfaceToSurfacePDUGenerator

def ensure_output_dir(dir_path):
    """Ensure the output directory exists, create it if not"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path

def generate_air_to_air_scenarios(output_dir):
    """Generate all air-to-air combat scenarios"""
    print("="*80)
    print("Generating Air-to-Air Combat Scenarios")
    print("="*80)
    
    generator = AirToAirPDUGenerator()
    
    # Dogfight scenario
    print("\nGenerating dogfight scenario...")
    generator.generate_dogfight_scenario(duration=120.0, num_blue=2, num_red=2)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/air_to_air_dogfight_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/air_to_air_dogfight_pdus.parquet")
    
    # Intercept scenario
    print("\nGenerating intercept scenario...")
    generator = AirToAirPDUGenerator()  # Create a new generator
    generator.generate_intercept_scenario(duration=120.0)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/air_to_air_intercept_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/air_to_air_intercept_pdus.parquet")
    
    # Additional scenario with more aircraft
    print("\nGenerating large-scale dogfight scenario...")
    generator = AirToAirPDUGenerator()  # Create a new generator
    generator.generate_dogfight_scenario(duration=180.0, num_blue=4, num_red=4)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/air_to_air_large_dogfight_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/air_to_air_large_dogfight_pdus.parquet")
    
    print("\nAir-to-Air scenarios complete.")

def generate_air_to_surface_scenarios(output_dir):
    """Generate all air-to-surface combat scenarios"""
    print("="*80)
    print("Generating Air-to-Surface Combat Scenarios")
    print("="*80)
    
    generator = AirToSurfacePDUGenerator()
    
    # Strike mission scenario
    print("\nGenerating strike mission scenario...")
    generator.generate_strike_mission_scenario(duration=300.0, num_aircraft=3, num_targets=5)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/air_to_surface_strike_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/air_to_surface_strike_pdus.parquet")
    
    # Dynamic battlefield scenario with command and control
    print("\nGenerating dynamic battlefield scenario...")
    generator = AirToSurfacePDUGenerator()  # Create a new generator
    generator.generate_dynamic_battlefield_scenario(duration=600.0, num_aircraft=4, num_targets=8)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/air_to_surface_dynamic_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/air_to_surface_dynamic_pdus.parquet")
    
    print("\nAir-to-Surface scenarios complete.")

def generate_surface_to_air_scenarios(output_dir):
    """Generate all surface-to-air combat scenarios"""
    print("="*80)
    print("Generating Surface-to-Air Combat Scenarios")
    print("="*80)
    
    generator = SurfaceToAirPDUGenerator()
    
    # Single SAM vs aircraft scenario
    print("\nGenerating single SAM vs aircraft scenario...")
    generator.generate_single_sam_vs_aircraft_scenario(duration=120.0)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/surface_to_air_single_sam_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/surface_to_air_single_sam_pdus.parquet")
    
    # Air defense network scenario
    print("\nGenerating air defense network scenario...")
    generator = SurfaceToAirPDUGenerator()  # Create a new generator
    generator.generate_air_defense_network_scenario(duration=180.0, num_sam_sites=3, num_aircraft=4)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/surface_to_air_network_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/surface_to_air_network_pdus.parquet")
    
    print("\nSurface-to-Air scenarios complete.")

def generate_surface_to_surface_scenarios(output_dir):
    """Generate all surface-to-surface combat scenarios"""
    print("="*80)
    print("Generating Surface-to-Surface Combat Scenarios")
    print("="*80)
    
    generator = SurfaceToSurfacePDUGenerator()
    
    # Naval battle scenario
    print("\nGenerating naval battle scenario...")
    generator.generate_naval_battle_scenario(duration=300.0, num_blue_ships=2, num_red_ships=2)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/surface_to_surface_naval_battle_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/surface_to_surface_naval_battle_pdus.parquet")
    
    # Coastal raid scenario
    print("\nGenerating coastal raid scenario...")
    generator = SurfaceToSurfacePDUGenerator()  # Create a new generator
    generator.generate_coastal_raid_scenario(duration=240.0, num_blue_ships=1, num_red_vehicles=4)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/surface_to_surface_coastal_raid_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/surface_to_surface_coastal_raid_pdus.parquet")
    
    # Land battle scenario
    print("\nGenerating land battle scenario...")
    generator = SurfaceToSurfacePDUGenerator()  # Create a new generator
    generator.generate_land_battle_scenario(duration=240.0, num_blue_vehicles=4, num_red_vehicles=4)
    
    # Save the generated PDUs
    generator.save_to_pickle(f"{output_dir}/surface_to_surface_land_battle_pdus.pkl")
    generator.save_to_parquet(f"{output_dir}/surface_to_surface_land_battle_pdus.parquet")
    
    print("\nSurface-to-Surface scenarios complete.")

def merge_datasets(output_dir):
    """Merge all scenario datasets into one combined dataset"""
    print("="*80)
    print("Merging All Datasets")
    print("="*80)
    
    # Get all parquet files
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    
    if not parquet_files:
        print("No parquet files found to merge.")
        return
    
    # Load and concatenate all dataframes
    dfs = []
    for pq_file in parquet_files:
        print(f"Loading {pq_file}...")
        file_path = os.path.join(output_dir, pq_file)
        df = pd.read_parquet(file_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    print("Concatenating dataframes...")
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Save combined dataset
    print(f"Saving combined dataset with {len(combined_df)} PDUs...")
    combined_df.to_parquet(f"{output_dir}/all_scenarios_combined.parquet", index=False)
    
    print("Dataset merging complete.")

def create_test_train_split(output_dir, test_size=0.2):
    """Create test/train split from the combined dataset"""
    print("="*80)
    print("Creating Test/Train Split")
    print("="*80)
    
    combined_file = f"{output_dir}/all_scenarios_combined.parquet"
    if not os.path.exists(combined_file):
        print(f"Combined dataset file {combined_file} not found.")
        return
    
    # Load combined dataset
    print("Loading combined dataset...")
    df = pd.read_parquet(combined_file)
    
    # Shuffle the data
    print("Shuffling data...")
    df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)
    
    # Calculate split index
    split_idx = int(len(df) * (1 - test_size))
    
    # Split the data
    print(f"Splitting data with test_size={test_size}...")
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    
    # Save train and test sets
    print(f"Saving train set with {len(train_df)} PDUs...")
    train_df.to_parquet(f"{output_dir}/train_set.parquet", index=False)
    
    print(f"Saving test set with {len(test_df)} PDUs...")
    test_df.to_parquet(f"{output_dir}/test_set.parquet", index=False)
    
    print("Test/train split complete.")

def generate_integrity_reports(output_dir):
    """Generate reports on the dataset for integrity verification"""
    print("="*80)
    print("Generating Data Integrity Reports")
    print("="*80)
    
    # Check if combined dataset exists
    combined_file = f"{output_dir}/all_scenarios_combined.parquet"
    if not os.path.exists(combined_file):
        print(f"Combined dataset file {combined_file} not found.")
        return
    
    # Load combined dataset
    print("Loading combined dataset...")
    df = pd.read_parquet(combined_file)
    
    # Create report directory
    report_dir = f"{output_dir}/reports"
    ensure_output_dir(report_dir)
    
    # General dataset statistics
    print("Generating general dataset statistics...")
    with open(f"{report_dir}/dataset_stats.txt", 'w') as f:
        f.write(f"Dataset Generation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total PDUs: {len(df)}\n")
        f.write(f"Total Unique Entity IDs: {df['entity_id'].nunique()}\n")
        f.write(f"PDU Types Distribution:\n")
        pdu_type_counts = df['pdu_type'].value_counts()
        for pdu_type, count in pdu_type_counts.items():
            f.write(f"  {pdu_type}: {count} ({count/len(df)*100:.2f}%)\n")
        
        f.write(f"\nScenarios Distribution:\n")
        scenario_counts = df['scenario'].value_counts()
        for scenario, count in scenario_counts.items():
            f.write(f"  {scenario}: {count} ({count/len(df)*100:.2f}%)\n")
        
        f.write(f"\nForce ID Distribution:\n")
        force_counts = df['force_id'].value_counts()
        for force_id, count in force_counts.items():
            f.write(f"  {force_id}: {count} ({count/len(df)*100:.2f}%)\n")
        
        f.write(f"\nEntity Kind Distribution:\n")
        kind_counts = df['entity_kind'].value_counts()
        for kind, count in kind_counts.items():
            f.write(f"  {kind}: {count} ({count/len(df)*100:.2f}%)\n")
        
        f.write(f"\nEntity Domain Distribution:\n")
        domain_counts = df['entity_domain'].value_counts()
        for domain, count in domain_counts.items():
            f.write(f"  {domain}: {count} ({count/len(df)*100:.2f}%)\n")
    
    # Entity ID statistics - find all entities and their interactions
    print("Generating entity interaction statistics...")
    entity_stats = {}
    for _, row in df.iterrows():
        entity_id = row['entity_id']
        
        if entity_id not in entity_stats:
            entity_stats[entity_id] = {
                'pdu_count': 0,
                'first_timestamp': float('inf'),
                'last_timestamp': 0,
                'pdu_types': set(),
                'interactions_as_shooter': set(),
                'interactions_as_target': set()
            }
        
        stats = entity_stats[entity_id]
        stats['pdu_count'] += 1
        stats['first_timestamp'] = min(stats['first_timestamp'], row['timestamp'])
        stats['last_timestamp'] = max(stats['last_timestamp'], row['timestamp'])
        stats['pdu_types'].add(row['pdu_type'])
        
        # Track interactions
        if 'shooter_id' in row and not pd.isna(row['shooter_id']) and row['shooter_id'] == entity_id:
            if 'target_id' in row and not pd.isna(row['target_id']):
                stats['interactions_as_shooter'].add(int(row['target_id']))
                
        if 'target_id' in row and not pd.isna(row['target_id']) and row['target_id'] == entity_id:
            if 'shooter_id' in row and not pd.isna(row['shooter_id']):
                stats['interactions_as_target'].add(int(row['shooter_id']))
    
    # Write entity stats to file
    with open(f"{report_dir}/entity_interactions.txt", 'w') as f:
        f.write("Entity Interaction Statistics:\n")
        f.write("="*50 + "\n")
        
        for entity_id, stats in entity_stats.items():
            f.write(f"Entity ID: {entity_id}\n")
            f.write(f"  PDU Count: {stats['pdu_count']}\n")
            f.write(f"  Lifetime: {stats['first_timestamp']:.1f} to {stats['last_timestamp']:.1f} " + 
                    f"({stats['last_timestamp'] - stats['first_timestamp']:.1f} seconds)\n")
            f.write(f"  PDU Types: {', '.join(stats['pdu_types'])}\n")
            
            if stats['interactions_as_shooter']:
                f.write(f"  Targeted Entities: {', '.join(map(str, stats['interactions_as_shooter']))}\n")
            else:
                f.write(f"  Targeted Entities: None\n")
                
            if stats['interactions_as_target']:
                f.write(f"  Targeted By Entities: {', '.join(map(str, stats['interactions_as_target']))}\n")
            else:
                f.write(f"  Targeted By Entities: None\n")
            
            f.write("\n")
    
    # Generate PDU sequence diagram for each scenario
    print("Generating PDU sequence diagrams...")
    for scenario in scenario_counts.index:
        scenario_df = df[df['scenario'] == scenario]
        
        with open(f"{report_dir}/{scenario}_sequence.txt", 'w') as f:
            f.write(f"PDU Sequence for Scenario: {scenario}\n")
            f.write("="*50 + "\n")
            f.write("Time  |  Entity  |  PDU Type  |  Details\n")
            f.write("-"*50 + "\n")
            
            # Sort by timestamp
            scenario_df = scenario_df.sort_values('timestamp')
            
            # Write sequence (limit to first 1000 PDUs to avoid huge files)
            for _, row in scenario_df.head(1000).iterrows():
                details = ""
                if row['pdu_type'] == 'FirePdu' and 'target_id' in row and not pd.isna(row['target_id']):
                    details = f"Target: {int(row['target_id'])}"
                elif row['pdu_type'] == 'DetonationPdu' and 'detonation_result' in row:
                    details = f"Result: {row['detonation_result']}"
                elif row['pdu_type'] == 'SignalPdu' and 'command_type' in row:
                    details = f"Command: {row['command_type']}"
                
                f.write(f"{row['timestamp']:6.1f} | {row['entity_id']:7d} | {row['pdu_type']:10s} | {details}\n")
                
            # If we truncated the output, note that
            if len(scenario_df) > 1000:
                f.write(f"\n... Truncated output. {len(scenario_df) - 1000} additional PDUs not shown.\n")
    
    # Generate breakdown by command messages (if present in dataset)
    if 'command_type' in df.columns:
        print("Generating command message statistics...")
        command_df = df[df['pdu_type'] == 'SignalPdu']
        
        with open(f"{report_dir}/command_messages.txt", 'w') as f:
            f.write("Command Message Statistics:\n")
            f.write("="*50 + "\n")
            
            if len(command_df) > 0:
                f.write(f"Total Command Messages: {len(command_df)}\n\n")
                
                f.write("Command Type Distribution:\n")
                command_counts = command_df['command_type'].value_counts()
                for cmd_type, count in command_counts.items():
                    f.write(f"  {cmd_type}: {count} ({count/len(command_df)*100:.2f}%)\n")
                
                f.write("\nCommand Timeline:\n")
                command_timeline = command_df.sort_values('timestamp')
                for _, row in command_timeline.iterrows():
                    f.write(f"  {row['timestamp']:6.1f} | Entity {row['entity_id']} to Entity {row['target_entity_id']} | {row['command_type']}\n")
            else:
                f.write("No command messages found in dataset.\n")
    
    print("Data integrity reports complete.")

def main():
    """Main function to run the complete data generation pipeline"""
    parser = argparse.ArgumentParser(description='Generate DIS PDU datasets for various combat scenarios')
    parser.add_argument('--output-dir', type=str, default='./dis_pdu_data',
                        help='Directory to store the generated datasets (default: ./dis_pdu_data)')
    parser.add_argument('--scenarios', type=str, default='all',
                        choices=['all', 'air-to-air', 'air-to-surface', 'surface-to-air', 'surface-to-surface'],
                        help='Which scenario types to generate (default: all)')
    parser.add_argument('--no-merge', action='store_true',
                        help='Skip merging individual datasets')
    parser.add_argument('--no-split', action='store_true',
                        help='Skip creating test/train split')
    parser.add_argument('--no-reports', action='store_true',
                        help='Skip generating data integrity reports')
    parser.add_argument('--test-size', type=float, default=0.2,
                        help='Proportion of data to use for testing (default: 0.2)')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = ensure_output_dir(args.output_dir)
    print(f"Saving all generated data to: {output_dir}")
    
    # Generate scenarios
    start_time = time.time()
    
    if args.scenarios in ['all', 'air-to-air']:
        generate_air_to_air_scenarios(output_dir)
    
    if args.scenarios in ['all', 'air-to-surface']:
        generate_air_to_surface_scenarios(output_dir)
    
    if args.scenarios in ['all', 'surface-to-air']:
        generate_surface_to_air_scenarios(output_dir)
    
    if args.scenarios in ['all', 'surface-to-surface']:
        generate_surface_to_surface_scenarios(output_dir)
    
    # Merge datasets if requested
    if not args.no_merge:
        merge_datasets(output_dir)
    
    # Create test/train split if requested
    if not args.no_merge and not args.no_split:
        create_test_train_split(output_dir, args.test_size)
    
    # Generate data integrity reports if requested
    if not args.no_merge and not args.no_reports:
        generate_integrity_reports(output_dir)
    
    elapsed_time = time.time() - start_time
    print(f"\nData generation complete in {elapsed_time:.2f} seconds")
    print(f"All data saved to: {output_dir}")
    
    # Output summary of what was generated
    print("\nDataset Summary:")
    print("-" * 30)
    
    # Count total PDUs generated
    total_pdus = 0
    scenario_counts = {}
    for filename in os.listdir(output_dir):
        if filename.endswith('.parquet') and not filename.startswith('all_') and not filename.startswith('train_') and not filename.startswith('test_'):
            # Extract scenario type from filename
            scenario_type = '_'.join(filename.split('_')[:3])
            if scenario_type not in scenario_counts:
                scenario_counts[scenario_type] = 0
                
            # Count PDUs in this file
            filepath = os.path.join(output_dir, filename)
            df = pd.read_parquet(filepath)
            file_pdus = len(df)
            total_pdus += file_pdus
            scenario_counts[scenario_type] += file_pdus
            
            print(f"  {filename}: {file_pdus} PDUs")
    
    print("-" * 30)
    print(f"Total PDUs by scenario type:")
    for scenario_type, count in scenario_counts.items():
        print(f"  {scenario_type}: {count} PDUs ({count/total_pdus*100:.1f}%)")
    
    print("-" * 30)
    print(f"Grand Total: {total_pdus} PDUs")
    
    # If we created combined dataset, show its stats
    combined_file = os.path.join(output_dir, 'all_scenarios_combined.parquet')
    if os.path.exists(combined_file):
        df = pd.read_parquet(combined_file)
        print(f"\nCombined Dataset:")
        print(f"  Total PDUs: {len(df)}")
        print(f"  Entity types: {df['entity_kind'].nunique()}")
        print(f"  PDU types: {df['pdu_type'].nunique()}")
        print(f"  Unique entities: {df['entity_id'].nunique()}")
    
    print("\nGeneration complete. Use the dataset for training models to verify PDU syntax and semantics.")


if __name__ == "__main__":
    main()