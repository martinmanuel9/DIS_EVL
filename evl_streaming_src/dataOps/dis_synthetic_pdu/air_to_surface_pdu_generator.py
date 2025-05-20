#!/usr/bin/python

"""
Air-to-Surface Combat DIS PDU Generator

This script generates DIS PDUs for air-to-surface combat scenarios.
It creates Entity State PDUs for aircraft and ground targets, 
along with Fire/Detonation PDUs for air-to-ground munitions.

The script can generate a parquet file or pickle file with labeled PDUs for training a model.
"""

import os
import time
import math
import random
import pickle
import numpy as np
import pandas as pd
from io import BytesIO
from datetime import datetime

# Import Open DIS classes
from opendis.dis7 import *
from opendis.DataInputStream import DataInputStream
from opendis.DataOutputStream import DataOutputStream

class AircraftType:
    """Constants for aircraft types"""
    # Entity Kind = Platform (1), Domain = Air (2), Country = USA (225)
    F16 = EntityType(1, 2, 225, 1, 1, 0, 0)  # F-16 Fighting Falcon
    F15E = EntityType(1, 2, 225, 1, 2, 0, 0)  # F-15E Strike Eagle
    A10 = EntityType(1, 2, 225, 1, 4, 0, 0)  # A-10 Thunderbolt II
    F35 = EntityType(1, 2, 225, 1, 3, 0, 0)  # F-35 Lightning II
    
    # Entity Kind = Platform (1), Domain = Air (2), Country = Russia (222)
    SU25 = EntityType(1, 2, 222, 1, 7, 0, 0)  # Su-25 Frogfoot
    SU34 = EntityType(1, 2, 222, 1, 14, 0, 0)  # Su-34 Fullback
    
    @staticmethod
    def get_random_us_aircraft():
        """Get a random US aircraft type"""
        return random.choice([AircraftType.F16, AircraftType.F15E, 
                              AircraftType.A10, AircraftType.F35])
    
    @staticmethod
    def get_random_russian_aircraft():
        """Get a random Russian aircraft type"""
        return random.choice([AircraftType.SU25, AircraftType.SU34])

class GroundTargetType:
    """Constants for ground target types"""
    # Entity Kind = Platform (1), Domain = Land (3), Country = Various
    
    # Military Vehicles (Country = Generic/Unspecified)
    TANK = EntityType(1, 3, 0, 1, 1, 0, 0)  # Main Battle Tank
    APC = EntityType(1, 3, 0, 2, 1, 0, 0)   # Armored Personnel Carrier
    SAM = EntityType(1, 3, 0, 6, 1, 0, 0)   # SAM Launcher
    RADAR = EntityType(1, 3, 0, 12, 1, 0, 0)  # Radar System
    
    # Buildings/Structures
    BUILDING = EntityType(1, 3, 0, 20, 1, 0, 0)  # Generic Building
    BRIDGE = EntityType(1, 3, 0, 20, 2, 0, 0)    # Bridge
    BUNKER = EntityType(1, 3, 0, 20, 3, 0, 0)    # Bunker
    
    @staticmethod
    def get_random_military_target():
        """Get a random military target type"""
        return random.choice([
            GroundTargetType.TANK, GroundTargetType.APC, 
            GroundTargetType.SAM, GroundTargetType.RADAR
        ])
    
    @staticmethod
    def get_random_structure_target():
        """Get a random structure target type"""
        return random.choice([
            GroundTargetType.BUILDING, GroundTargetType.BRIDGE, 
            GroundTargetType.BUNKER
        ])

class AirToGroundMunitionType:
    """Constants for air-to-ground munition types"""
    # Entity Kind = Munition (2), Domain = Anti-Surface (4), Country = USA (225)
    GBU12 = EntityType(2, 4, 225, 1, 1, 0, 0)   # GBU-12 Paveway II - Laser Guided Bomb
    GBU31 = EntityType(2, 4, 225, 1, 2, 0, 0)   # GBU-31 JDAM - GPS Guided Bomb
    AGM65 = EntityType(2, 4, 225, 3, 1, 0, 0)   # AGM-65 Maverick - Air-to-Ground Missile
    AGM114 = EntityType(2, 4, 225, 3, 2, 0, 0)  # AGM-114 Hellfire
    CBU97 = EntityType(2, 4, 225, 2, 1, 0, 0)   # CBU-97 Cluster Bomb
    
    # Entity Kind = Munition (2), Domain = Anti-Surface (4), Country = Russia (222)
    KAB500 = EntityType(2, 4, 222, 1, 1, 0, 0)  # KAB-500L Guided Bomb
    KH29 = EntityType(2, 4, 222, 3, 1, 0, 0)    # Kh-29 Air-to-Surface Missile
    
    @staticmethod
    def get_random_us_munition():
        """Get a random US air-to-ground munition type"""
        return random.choice([
            AirToGroundMunitionType.GBU12, AirToGroundMunitionType.GBU31,
            AirToGroundMunitionType.AGM65, AirToGroundMunitionType.AGM114,
            AirToGroundMunitionType.CBU97
        ])
    
    @staticmethod
    def get_random_russian_munition():
        """Get a random Russian air-to-ground munition type"""
        return random.choice([
            AirToGroundMunitionType.KAB500, AirToGroundMunitionType.KH29
        ])

class Force:
    """Force ID constants"""
    FRIENDLY = 1  # Blue force
    OPPOSING = 2  # Red force
    NEUTRAL = 3   # White/neutral force

class AirToSurfacePDUGenerator:
    """Generator class for Air-to-Surface Combat PDUs"""
    
    def __init__(self, exercise_id=1, site_id=1, application_id=1):
        """Initialize the generator"""
        self.exercise_id = exercise_id
        self.site_id = site_id
        self.application_id = application_id
        self.entity_counter = 1
        self.event_counter = 1
        self.current_time = 0
        self.time_step = 1.0  # seconds
        self.pdu_store = []  # Store all generated PDUs
        
        # Scenario data
        self.entities = {}  # Dictionary to track entities by ID
        
    def create_entity_id(self):
        """Create a unique entity ID"""
        entity_id = EntityID()
        entity_id.siteID = self.site_id
        entity_id.applicationID = self.application_id
        entity_id.entityID = self.entity_counter
        self.entity_counter += 1
        return entity_id
    
    def create_event_id(self):
        """Create a unique event ID"""
        event_id = EventIdentifier()
        event_id.eventNumber = self.event_counter
        event_id.simulationAddress.siteID = self.site_id
        event_id.simulationAddress.applicationID = self.application_id
        self.event_counter += 1
        return event_id
    
    def create_aircraft(self, aircraft_type, force_id, initial_position, initial_velocity):
        """Create an aircraft entity"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the aircraft
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = aircraft_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = initial_position[0]
        espdu.entityLocation.y = initial_position[1]
        espdu.entityLocation.z = initial_position[2]
        
        # Set velocity (X, Y, Z in meters/sec)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = initial_velocity[0]
        espdu.entityLinearVelocity.y = initial_velocity[1]
        espdu.entityLinearVelocity.z = initial_velocity[2]
        
        # Set orientation (Psi, Theta, Phi in radians)
        # Psi=Yaw, Theta=Pitch, Phi=Roll
        heading = math.atan2(initial_velocity[1], initial_velocity[0])
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = heading
        espdu.entityOrientation.theta = 0.0  # Level flight
        espdu.entityOrientation.phi = 0.0    # No roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"AC{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'aircraft',
            'entity_type': aircraft_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': initial_position.copy(),
            'velocity': initial_velocity.copy(),
            'orientation': [heading, 0.0, 0.0],
            'alive': True
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': aircraft_type.entityKind,
            'entity_domain': aircraft_type.entityDomain,
            'entity_country': aircraft_type.entityCountry,
            'entity_category': aircraft_type.entityCategory,
            'entity_subcategory': aircraft_type.entitySubcategory,
            'position': initial_position.copy(),
            'velocity': initial_velocity.copy(),
            'orientation': [heading, 0.0, 0.0],
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def create_ground_target(self, target_type, force_id, position):
        """Create a ground target entity"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the ground target
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = target_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]
        
        # Set velocity (stationary)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = 0.0
        espdu.entityLinearVelocity.y = 0.0
        espdu.entityLinearVelocity.z = 0.0
        
        # Set orientation
        # Random heading for visual diversity
        heading = random.uniform(0, 2*math.pi)
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = heading
        espdu.entityOrientation.theta = 0.0  # Level
        espdu.entityOrientation.phi = 0.0    # No roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 0  # DRM_STATIC: Static/Stationary
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"TGT{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'ground_target',
            'entity_type': target_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': position.copy(),
            'velocity': [0.0, 0.0, 0.0],
            'orientation': [heading, 0.0, 0.0],
            'alive': True,
            'damaged': False  # Track if target has been hit but not destroyed
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': target_type.entityKind,
            'entity_domain': target_type.entityDomain,
            'entity_country': target_type.entityCountry,
            'entity_category': target_type.entityCategory,
            'entity_subcategory': target_type.entitySubcategory,
            'position': position.copy(),
            'velocity': [0.0, 0.0, 0.0],
            'orientation': [heading, 0.0, 0.0],
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def update_aircraft(self, entity_id, new_position, new_velocity, new_orientation=None):
        """Update an aircraft's state"""
        if entity_id not in self.entities or not self.entities[entity_id]['alive']:
            return None
        
        entity = self.entities[entity_id]
        
        # Create new Entity State PDU
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity['entity_id']
        espdu.forceId = entity['force_id']
        espdu.entityType = entity['entity_type']
        
        # Update position
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = new_position[0]
        espdu.entityLocation.y = new_position[1]
        espdu.entityLocation.z = new_position[2]
        
        # Update velocity
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = new_velocity[0]
        espdu.entityLinearVelocity.y = new_velocity[1]
        espdu.entityLinearVelocity.z = new_velocity[2]
        
        # Update orientation
        if new_orientation:
            heading, pitch, roll = new_orientation
        else:
            # Calculate heading from velocity
            heading = math.atan2(new_velocity[1], new_velocity[0])
            pitch = math.asin(new_velocity[2] / math.sqrt(new_velocity[0]**2 + new_velocity[1]**2 + new_velocity[2]**2)) if any(new_velocity) else 0
            roll = 0
            
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = heading
        espdu.entityOrientation.theta = pitch
        espdu.entityOrientation.phi = roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"AC{entity_id:02d}")
        
        # Update entity tracking data
        entity['position'] = new_position.copy()
        entity['velocity'] = new_velocity.copy()
        entity['orientation'] = [heading, pitch, roll]
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id,
            'force_id': entity['force_id'],
            'entity_kind': entity['entity_type'].entityKind,
            'entity_domain': entity['entity_type'].entityDomain,
            'entity_country': entity['entity_type'].entityCountry,
            'entity_category': entity['entity_type'].entityCategory,
            'entity_subcategory': entity['entity_type'].entitySubcategory,
            'position': new_position.copy(),
            'velocity': new_velocity.copy(),
            'orientation': [heading, pitch, roll],
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id
    
    def update_ground_target(self, target_id, damaged=False, destroyed=False):
        """Update a ground target's state (e.g., after damage)"""
        if target_id not in self.entities or not self.entities[target_id]['alive']:
            return None
        
        entity = self.entities[target_id]
        if entity['type'] != 'ground_target':
            return None
            
        # Only update if status changed
        if damaged and not entity['damaged']:
            entity['damaged'] = True
        elif destroyed:
            entity['alive'] = False
        else:
            return target_id  # No change needed
        
        # Create new Entity State PDU for the updated target
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity['entity_id']
        espdu.forceId = entity['force_id']
        espdu.entityType = entity['entity_type']
        
        # Position (unchanged)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = entity['position'][0]
        espdu.entityLocation.y = entity['position'][1]
        espdu.entityLocation.z = entity['position'][2]
        
        # Velocity (unchanged - still stationary)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = 0.0
        espdu.entityLinearVelocity.y = 0.0
        espdu.entityLinearVelocity.z = 0.0
        
        # Orientation (unchanged)
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = entity['orientation'][0]
        espdu.entityOrientation.theta = entity['orientation'][1]
        espdu.entityOrientation.phi = entity['orientation'][2]
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 0  # DRM_STATIC
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance to indicate damage or destruction
        # This is a simplified approach - actual appearance would use bit fields
        if destroyed:
            espdu.entityAppearance = 2  # Destroyed
        elif damaged:
            espdu.entityAppearance = 1  # Damaged
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"TGT{target_id:02d}")
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        status = "Destroyed" if destroyed else "Damaged" if damaged else "Normal"
        
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': target_id,
            'force_id': entity['force_id'],
            'entity_kind': entity['entity_type'].entityKind,
            'entity_domain': entity['entity_type'].entityDomain,
            'entity_country': entity['entity_type'].entityCountry,
            'entity_category': entity['entity_type'].entityCategory,
            'entity_subcategory': entity['entity_type'].entitySubcategory,
            'position': entity['position'].copy(),
            'velocity': [0.0, 0.0, 0.0],
            'orientation': entity['orientation'].copy(),
            'status': status,
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return target_id
    
    def fire_munition(self, aircraft_id, target_id, munition_type):
        """Fire a munition from an aircraft at a ground target"""
        if aircraft_id not in self.entities or not self.entities[aircraft_id]['alive']:
            return None
        
        if target_id not in self.entities:
            return None
            
        aircraft = self.entities[aircraft_id]
        target = self.entities[target_id]
        
        # Create munition entity
        munition_entity_id = self.create_entity_id()
        event_id = self.create_event_id()
        
        # Create Fire PDU
        fire_pdu = FirePdu()
        fire_pdu.protocolVersion = 7
        fire_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        fire_pdu.firingEntityID = aircraft['entity_id']
        
        # Set target entity
        fire_pdu.targetEntityID = target['entity_id']
        
        # Set event ID
        fire_pdu.eventID = event_id
        
        # Set munition ID
        fire_pdu.munitionExpendableID = munition_entity_id
        
        # Set fire mission index
        fire_pdu.fireMissionIndex = 0
        
        # Set location (same as aircraft's position)
        fire_pdu.location = Vector3Double()
        fire_pdu.location.x = aircraft['position'][0]
        fire_pdu.location.y = aircraft['position'][1]
        fire_pdu.location.z = aircraft['position'][2]
        
        # Set descriptor
        fire_pdu.descriptor = MunitionDescriptor()
        fire_pdu.descriptor.munitionType = munition_type
        fire_pdu.descriptor.warhead = 1000  # Conventional warhead
        fire_pdu.descriptor.fuse = 1000     # Proximity fuse
        fire_pdu.descriptor.quantity = 1
        fire_pdu.descriptor.rate = 0
        
        # Calculate initial velocity vector toward target
        dx = target['position'][0] - aircraft['position'][0]
        dy = target['position'][1] - aircraft['position'][1]
        dz = target['position'][2] - aircraft['position'][2]
        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
        
        # Normalize direction
        if distance > 0:
            dx /= distance
            dy /= distance
            dz /= distance
        
        # Set velocity based on munition type
        speed = 300.0  # Default speed in m/s
        
        # Guided bombs are slower
        if munition_type.entityCategory == 1:  # Guided bombs category
            speed = 200.0
        # Missiles are faster    
        elif munition_type.entityCategory == 3:  # Missile category
            speed = 400.0
        
        munition_velocity = [
            dx * speed,
            dy * speed,
            dz * speed
        ]
        
        fire_pdu.velocity = Vector3Float()
        fire_pdu.velocity.x = munition_velocity[0]
        fire_pdu.velocity.y = munition_velocity[1]
        fire_pdu.velocity.z = munition_velocity[2]
        
        # Set range
        fire_pdu.range = distance
        
        # Create munition entity in our tracking system
        self.entities[munition_entity_id.entityID] = {
            'type': 'munition',
            'entity_type': munition_type,
            'entity_id': munition_entity_id,
            'force_id': aircraft['force_id'],
            'position': aircraft['position'].copy(),
            'velocity': munition_velocity.copy(),
            'orientation': [math.atan2(dy, dx), math.asin(-dz) if abs(dz) < 1 else 0, 0],
            'shooter_id': aircraft_id,
            'target_id': target_id,
            'event_id': event_id,
            'alive': True,
            'flight_time': 0.0,
            'impact_time': distance / speed  # Estimate time to impact
        }
        
        # Get munition name based on type
        munition_name = self.get_munition_name(munition_type)
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(fire_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'FirePdu',
            'entity_id': munition_entity_id.entityID,
            'force_id': aircraft['force_id'],
            'shooter_id': aircraft_id,
            'target_id': target_id,
            'entity_kind': munition_type.entityKind,
            'entity_domain': munition_type.entityDomain,
            'entity_country': munition_type.entityCountry,
            'entity_category': munition_type.entityCategory,
            'entity_subcategory': munition_type.entitySubcategory,
            'munition_name': munition_name,
            'position': aircraft['position'].copy(),
            'velocity': munition_velocity.copy(),
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return munition_entity_id.entityID
    
    def update_munition(self, munition_id):
        """Update a munition's state during flight"""
        if munition_id not in self.entities or not self.entities[munition_id]['alive']:
            return None
            
        munition = self.entities[munition_id]
        if munition['type'] != 'munition':
            return None
        
        # Create Entity State PDU for the munition
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = munition['entity_id']
        espdu.forceId = munition['force_id']
        espdu.entityType = munition['entity_type']
        
        # Set position
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = munition['position'][0]
        espdu.entityLocation.y = munition['position'][1]
        espdu.entityLocation.z = munition['position'][2]
        
        # Set velocity
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = munition['velocity'][0]
        espdu.entityLinearVelocity.y = munition['velocity'][1]
        espdu.entityLinearVelocity.z = munition['velocity'][2]
        
        # Set orientation
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = munition['orientation'][0]
        espdu.entityOrientation.theta = munition['orientation'][1]
        espdu.entityOrientation.phi = munition['orientation'][2]
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 4  # DRM_RVW: Rate Velocity World
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"MUN{munition_id:02d}")
        
        # Get munition name
        munition_name = self.get_munition_name(munition['entity_type'])
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': munition_id,
            'force_id': munition['force_id'],
            'entity_kind': munition['entity_type'].entityKind,
            'entity_domain': munition['entity_type'].entityDomain,
            'entity_country': munition['entity_type'].entityCountry,
            'entity_category': munition['entity_type'].entityCategory,
            'entity_subcategory': munition['entity_type'].entitySubcategory,
            'munition_name': munition_name,
            'position': munition['position'].copy(),
            'velocity': munition['velocity'].copy(),
            'orientation': munition['orientation'].copy(),
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return munition_id
    
    def detonate_munition(self, munition_id, detonation_result=0):
        """Detonate a munition near or on a target"""
        if munition_id not in self.entities or not self.entities[munition_id]['alive']:
            return None
            
        munition = self.entities[munition_id]
        if munition['type'] != 'munition':
            return None
            
        target_id = munition['target_id']
        if target_id not in self.entities:
            return None
            
        target = self.entities[target_id]
        
        # Create Detonation PDU
        detonation_pdu = DetonationPdu()
        detonation_pdu.protocolVersion = 7
        detonation_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        shooter_id = munition['shooter_id']
        if shooter_id in self.entities:
            detonation_pdu.firingEntityID = self.entities[shooter_id]['entity_id']
        
        # Set target entity
        detonation_pdu.targetEntityID = target['entity_id']
        
        # Set munition ID
        detonation_pdu.munitionExpendableID = munition['entity_id']
        
        # Set event ID (same as the Fire PDU)
        detonation_pdu.eventID = munition['event_id']
        
        # Set location - either at target (direct hit) or near target (miss)
        is_direct_hit = (detonation_result == 1 or detonation_result == 7)
        
        # Location in world coordinates
        detonation_pdu.locationInWorldCoordinates = Vector3Double()
        
        if is_direct_hit:
            # Direct hit - use target position with minimal offset
            detonation_pdu.locationInWorldCoordinates.x = target['position'][0] + random.uniform(-2, 2)
            detonation_pdu.locationInWorldCoordinates.y = target['position'][1] + random.uniform(-2, 2)
            detonation_pdu.locationInWorldCoordinates.z = target['position'][2] + random.uniform(0, 1)
        else:
            # Miss - use position near target
            detonation_pdu.locationInWorldCoordinates.x = target['position'][0] + random.uniform(-30, 30)
            detonation_pdu.locationInWorldCoordinates.y = target['position'][1] + random.uniform(-30, 30)
            detonation_pdu.locationInWorldCoordinates.z = target['position'][2] + random.uniform(0, 1)
        
        # Set descriptor
        detonation_pdu.descriptor = MunitionDescriptor()
        detonation_pdu.descriptor.munitionType = munition['entity_type']
        detonation_pdu.descriptor.warhead = 1000  # Conventional warhead
        detonation_pdu.descriptor.fuse = 1000     # Proximity fuse
        detonation_pdu.descriptor.quantity = 1
        detonation_pdu.descriptor.rate = 0
        
        # Set velocity at detonation (reduced from flight velocity)
        speed_reduction = 0.3  # 70% reduction at impact
        detonation_pdu.velocity = Vector3Float()
        detonation_pdu.velocity.x = munition['velocity'][0] * speed_reduction
        detonation_pdu.velocity.y = munition['velocity'][1] * speed_reduction
        detonation_pdu.velocity.z = munition['velocity'][2] * speed_reduction
        
        # Set detonation result
        # 0=Other, 1=Entity Impact, 2=Entity Proximate Detonation, 3=Ground Impact, 4=Ground Proximate Detonation
        # 5=Detonation, 6=None/Dud, 7=HE hit, 8=HE proximate detonation
        detonation_pdu.detonationResult = detonation_result
        
        # Set location relative to entity
        if is_direct_hit:
            # For direct hits, calculate a precise impact point on the target
            detonation_pdu.locationInEntityCoordinates = Vector3Float()
            detonation_pdu.locationInEntityCoordinates.x = random.uniform(-3, 3)
            detonation_pdu.locationInEntityCoordinates.y = random.uniform(-3, 3)
            detonation_pdu.locationInEntityCoordinates.z = random.uniform(0, 2)
        else:
            # For near misses, just use a vector from target to detonation point
            detonation_pdu.locationInEntityCoordinates = Vector3Float()
            detonation_pdu.locationInEntityCoordinates.x = detonation_pdu.locationInWorldCoordinates.x - target['position'][0]
            detonation_pdu.locationInEntityCoordinates.y = detonation_pdu.locationInWorldCoordinates.y - target['position'][1]
            detonation_pdu.locationInEntityCoordinates.z = detonation_pdu.locationInWorldCoordinates.z - target['position'][2]
            
        # Mark munition as no longer alive
        munition['alive'] = False
        
        # If detonation was a direct hit or close enough, update target status
        if is_direct_hit:
            # Determine damage effect based on munition type and target type
            destroy_probability = 0.7  # Default 70% chance to destroy
            
            # Adjust probability based on target type (harder targets = lower probability)
            if target['entity_type'].entityCategory == 1:  # Main Battle Tank
                destroy_probability = 0.5  # Tanks are tougher
            elif target['entity_type'].entityCategory == 6:  # SAM system
                destroy_probability = 0.6
                
            # Adjust probability based on munition type
            if munition['entity_type'].entityCategory == 3:  # Missile
                destroy_probability += 0.2  # Missiles more effective
            elif munition['entity_type'].entityCategory == 2:  # Cluster bombs
                destroy_probability += 0.1  # Cluster munitions somewhat more effective
                
            # Apply the effect
            if random.random() < destroy_probability:
                # Target destroyed
                self.update_ground_target(target_id, destroyed=True)
            else:
                # Target damaged but not destroyed
                self.update_ground_target(target_id, damaged=True)
        elif detonation_result in [2, 4, 8]:  # Proximity detonations
            # Near miss might still cause some damage
            if random.random() < 0.3:  # 30% chance to damage
                self.update_ground_target(target_id, damaged=True)
        
        # Get munition name
        munition_name = self.get_munition_name(munition['entity_type'])
        
        # Create detonation result description
        result_descriptions = {
            0: "Other",
            1: "Direct Hit",
            2: "Near Miss",
            3: "Ground Impact",
            4: "Ground Proximity",
            5: "Detonation",
            6: "Dud",
            7: "HE Direct Hit",
            8: "HE Near Miss"
        }
        result_desc = result_descriptions.get(detonation_result, "Unknown")
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(detonation_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'DetonationPdu',
            'entity_id': munition_id,
            'force_id': munition['force_id'],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'entity_kind': munition['entity_type'].entityKind,
            'entity_domain': munition['entity_type'].entityDomain,
            'entity_country': munition['entity_type'].entityCountry,
            'entity_category': munition['entity_type'].entityCategory,
            'entity_subcategory': munition['entity_type'].entitySubcategory,
            'munition_name': munition_name,
            'position': [detonation_pdu.locationInWorldCoordinates.x, 
                        detonation_pdu.locationInWorldCoordinates.y, 
                        detonation_pdu.locationInWorldCoordinates.z],
            'velocity': [detonation_pdu.velocity.x,
                        detonation_pdu.velocity.y,
                        detonation_pdu.velocity.z],
            'detonation_result': detonation_result,
            'detonation_desc': result_desc,
            'direct_hit': is_direct_hit,
            'scenario': 'air-to-surface',
            'pdu_binary': pdu_data
        })
        
        return munition_id
    
    def get_munition_name(self, munition_type):
        """Get a human-readable name for a munition type"""
        # US Munitions
        if munition_type.entityCountry == 225:  # USA
            if munition_type.entityCategory == 1:  # Guided bombs
                if munition_type.entitySubcategory == 1:
                    return "GBU-12 Paveway II"
                elif munition_type.entitySubcategory == 2:
                    return "GBU-31 JDAM"
            elif munition_type.entityCategory == 2:  # Cluster bombs
                if munition_type.entitySubcategory == 1:
                    return "CBU-97"
            elif munition_type.entityCategory == 3:  # Missiles
                if munition_type.entitySubcategory == 1:
                    return "AGM-65 Maverick"
                elif munition_type.entitySubcategory == 2:
                    return "AGM-114 Hellfire"
        
        # Russian Munitions
        elif munition_type.entityCountry == 222:  # Russia
            if munition_type.entityCategory == 1:  # Guided bombs
                if munition_type.entitySubcategory == 1:
                    return "KAB-500L"
            elif munition_type.entityCategory == 3:  # Missiles
                if munition_type.entitySubcategory == 1:
                    return "Kh-29"
        
        # Generic fallback
        return f"Munition-{munition_type.entityCategory}-{munition_type.entitySubcategory}"
    
    def serialize_pdu(self, pdu):
        """Serialize a PDU to binary format"""
        memory_stream = BytesIO()
        output_stream = DataOutputStream(memory_stream)
        pdu.serialize(output_stream)
        return memory_stream.getvalue()
    
    def update_simulation(self):
        """Update the simulation by one time step"""
        self.current_time += self.time_step
        
        # Update all entities
        for entity_id, entity in list(self.entities.items()):
            if not entity['alive']:
                continue
                
            # For aircraft, update position based on velocity
            if entity['type'] == 'aircraft':
                new_position = [
                    entity['position'][0] + entity['velocity'][0] * self.time_step,
                    entity['position'][1] + entity['velocity'][1] * self.time_step,
                    entity['position'][2] + entity['velocity'][2] * self.time_step
                ]
                entity['position'] = new_position
                
                # Generate Entity State PDU periodically
                if self.current_time % 5.0 < self.time_step:
                    self.update_aircraft(entity_id, entity['position'], entity['velocity'], entity['orientation'])
            
            # For munitions, update position and check for impact
            elif entity['type'] == 'munition':
                # Update position
                new_position = [
                    entity['position'][0] + entity['velocity'][0] * self.time_step,
                    entity['position'][1] + entity['velocity'][1] * self.time_step,
                    entity['position'][2] + entity['velocity'][2] * self.time_step
                ]
                entity['position'] = new_position
                
                # Update flight time
                entity['flight_time'] += self.time_step
                
                # Generate Entity State PDU
                self.update_munition(entity_id)
                
                # Check if munition is near target
                target_id = entity['target_id']
                if target_id in self.entities and self.entities[target_id]['alive']:
                    target = self.entities[target_id]
                    
                    # Calculate distance to target
                    dx = target['position'][0] - entity['position'][0]
                    dy = target['position'][1] - entity['position'][1]
                    dz = target['position'][2] - entity['position'][2]
                    distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                    
                    # If close enough, detonate
                    if distance < 10.0:  # Within 10m
                        # Determine hit type
                        if distance < 3.0 and random.random() < 0.7:  # 70% chance of direct hit when very close
                            # Direct hit
                            detonation_result = 1  # Entity Impact
                            if entity['entity_type'].entityCategory == 3:  # If it's a missile
                                detonation_result = 7  # HE hit
                        else:
                            # Near miss
                            detonation_result = 2  # Entity Proximate Detonation
                            if entity['entity_type'].entityCategory == 3:  # If it's a missile
                                detonation_result = 8  # HE proximate detonation
                                
                        self.detonate_munition(entity_id, detonation_result)
                        continue
                
                # If munition is below ground level (assuming ground is at z=0), impact ground
                if entity['position'][2] <= 0:
                    if random.random() < 0.5:
                        detonation_result = 3  # Ground Impact
                    else:
                        detonation_result = 4  # Ground Proximate Detonation
                    
                    self.detonate_munition(entity_id, detonation_result)
                    continue
                    
                # Detonate munition if it's been flying too long (fuel exhaustion)
                if entity['flight_time'] > 60.0:  # 60 seconds max flight time
                    self.detonate_munition(entity_id, 6)  # 6=None/Dud
                    continue
    
    def generate_bombing_run_scenario(self, duration=180.0, num_aircraft=2, num_targets=5):
        """Generate a scenario with aircraft performing bombing runs on ground targets
        
        Args:
            duration: Duration of the scenario in seconds
            num_aircraft: Number of aircraft
            num_targets: Number of ground targets
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create aircraft (all friendly)
        aircraft_ids = []
        for i in range(num_aircraft):
            # Position aircraft at starting point
            position = [
                -20000.0 + random.uniform(-5000, 5000),  # Start west of targets
                random.uniform(-10000, 10000),           # Random north/south position
                3000.0 + random.uniform(0, 2000)         # Random altitude 3-5km
            ]
            
            # Initial velocity moving east toward targets
            speed = 250.0 + random.uniform(-50, 50)  # Aircraft speed varies
            velocity = [
                speed,                           # Moving east
                random.uniform(-20, 20),         # Small north/south drift
                random.uniform(-5, 5)            # Small vertical drift
            ]
            
            # Random US aircraft type
            aircraft_type = AircraftType.get_random_us_aircraft()
            
            # Create the aircraft
            entity_id = self.create_aircraft(aircraft_type, Force.FRIENDLY, position, velocity)
            aircraft_ids.append(entity_id)
        
        # Create ground targets (all opposing)
        target_ids = []
        for i in range(num_targets):
            # Position targets in a scattered formation
            position = [
                random.uniform(0, 10000),       # East side of battlefield
                random.uniform(-5000, 5000),    # Random north/south position
                0.0                             # Ground level
            ]
            
            # Alternate between military vehicles and structures
            if i % 2 == 0:
                target_type = GroundTargetType.get_random_military_target()
            else:
                target_type = GroundTargetType.get_random_structure_target()
            
            # Create the target
            entity_id = self.create_ground_target(target_type, Force.OPPOSING, position)
            target_ids.append(entity_id)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            # Update the simulation
            self.update_simulation()
            
            # For each aircraft, check if it's in range to attack targets
            for aircraft_id in aircraft_ids:
                if aircraft_id not in self.entities or not self.entities[aircraft_id]['alive']:
                    continue
                    
                aircraft = self.entities[aircraft_id]
                
                # Find alive targets
                alive_targets = [tid for tid in target_ids 
                                if tid in self.entities and self.entities[tid]['alive']]
                
                if not alive_targets:
                    continue  # No targets left
                
                # Find the closest target
                closest_target = None
                min_distance = float('inf')
                
                for target_id in alive_targets:
                    target = self.entities[target_id]
                    dx = target['position'][0] - aircraft['position'][0]
                    dy = target['position'][1] - aircraft['position'][1]
                    dz = target['position'][2] - aircraft['position'][2]
                    distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                    
                    if distance < min_distance:
                        min_distance = distance
                        closest_target = target_id
                
                # If aircraft is within attack range (10km), consider attacking
                if min_distance < 10000 and random.random() < 0.05:  # 5% chance each step when in range
                    # Select random munition appropriate for the country
                    if aircraft['entity_type'].entityCountry == 225:  # US
                        munition_type = AirToGroundMunitionType.get_random_us_munition()
                    else:
                        munition_type = AirToGroundMunitionType.get_random_russian_munition()
                    
                    # Fire at the closest target
                    self.fire_munition(aircraft_id, closest_target, munition_type)
                
                # If aircraft has passed all targets, turn around for another pass
                eastmost_target_x = max(self.entities[tid]['position'][0] for tid in alive_targets)
                
                if aircraft['position'][0] > eastmost_target_x + 5000:  # 5km past eastmost target
                    # Turn around - reverse x velocity
                    aircraft['velocity'][0] = -aircraft['velocity'][0]
                    # Update orientation
                    aircraft['orientation'][0] = math.pi  # 180 degrees
                    
                # If aircraft has gone too far west, turn back east
                if aircraft['position'][0] < -30000:  # 30km west of origin
                    # Turn around - make x velocity positive
                    aircraft['velocity'][0] = abs(aircraft['velocity'][0])
                    # Update orientation
                    aircraft['orientation'][0] = 0  # 0 degrees
            
            # Check if all targets are destroyed
            alive_targets = [tid for tid in target_ids 
                            if tid in self.entities and self.entities[tid]['alive']]
            
            if not alive_targets:
                print(f"All targets destroyed at time {self.current_time:.1f}")
                # Continue for a bit to show aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Bombing run scenario completed at time {self.current_time:.1f} seconds")
        print(f"Targets destroyed: {num_targets - len(alive_targets)}/{num_targets}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def generate_precision_strike_scenario(self, duration=120.0):
        """Generate a scenario with a single aircraft performing precision strikes
        
        Args:
            duration: Duration of the scenario in seconds
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create a single strike aircraft
        position = [-15000.0, 0.0, 5000.0]  # Start position
        velocity = [200.0, 0.0, 0.0]        # Moving east
        aircraft_id = self.create_aircraft(AircraftType.F35, Force.FRIENDLY, position, velocity)
        
        # Create high-value targets
        targets = [
            {"type": GroundTargetType.SAM, "position": [0.0, 0.0, 0.0], "name": "SAM Site Alpha"},
            {"type": GroundTargetType.RADAR, "position": [1000.0, 500.0, 0.0], "name": "Radar Station Bravo"},
            {"type": GroundTargetType.BUNKER, "position": [2000.0, -500.0, 0.0], "name": "Command Bunker Charlie"}
        ]
        
        target_ids = []
        for target in targets:
            entity_id = self.create_ground_target(target['type'], Force.OPPOSING, target['position'])
            target_ids.append(entity_id)
            # Store target name
            self.entities[entity_id]['name'] = target['name']
        
        # Predefined attack sequence
        attack_sequence = [
            {"time": 30.0, "target_idx": 0, "munition": AirToGroundMunitionType.AGM65},  # Attack SAM first
            {"time": 60.0, "target_idx": 1, "munition": AirToGroundMunitionType.GBU12},  # Attack radar second
            {"time": 90.0, "target_idx": 2, "munition": AirToGroundMunitionType.GBU31}   # Attack bunker last
        ]
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            # Update the simulation
            self.update_simulation()
            
            # Check for scheduled attacks
            for attack in attack_sequence:
                if abs(self.current_time - attack['time']) < self.time_step:
                    target_idx = attack['target_idx']
                    if target_idx < len(target_ids):
                        target_id = target_ids[target_idx]
                        if target_id in self.entities and self.entities[target_id]['alive']:
                            self.fire_munition(aircraft_id, target_id, attack['munition'])
            
            # Aircraft behavior - orbit around target area if past all targets
            aircraft = self.entities[aircraft_id]
            if aircraft['position'][0] > 3000.0:  # Past all targets
                # Start orbiting - change to a circular path
                distance_from_center = math.sqrt(
                    (aircraft['position'][0] - 1000.0)**2 + 
                    (aircraft['position'][1])**2
                )
                
                if distance_from_center > 5000.0:
                    # Outside orbit radius, turn towards center
                    dx = 1000.0 - aircraft['position'][0]
                    dy = 0.0 - aircraft['position'][1]
                    angle = math.atan2(dy, dx)
                    
                    speed = math.sqrt(aircraft['velocity'][0]**2 + aircraft['velocity'][1]**2)
                    aircraft['velocity'][0] = speed * math.cos(angle)
                    aircraft['velocity'][1] = speed * math.sin(angle)
                    aircraft['orientation'][0] = angle
                else:
                    # Inside orbit radius, adjust velocity for circular path
                    # Calculate current angle from center
                    current_angle = math.atan2(
                        aircraft['position'][1], 
                        aircraft['position'][0] - 1000.0
                    )
                    
                    # Move perpendicular to radius
                    orbit_speed = 150.0  # Slower for orbiting
                    aircraft['velocity'][0] = -orbit_speed * math.sin(current_angle)
                    aircraft['velocity'][1] = orbit_speed * math.cos(current_angle)
                    aircraft['orientation'][0] = current_angle + math.pi/2
        
        print(f"Precision strike scenario completed at time {self.current_time:.1f} seconds")
        alive_targets = [tid for tid in target_ids if tid in self.entities and self.entities[tid]['alive']]
        print(f"Targets destroyed: {len(target_ids) - len(alive_targets)}/{len(target_ids)}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def save_to_pickle(self, filename="air_to_surface_pdus.pkl"):
        """Save the PDU data to a pickle file"""
        with open(filename, 'wb') as f:
            pickle.dump(self.pdu_store, f)
        print(f"Saved {len(self.pdu_store)} PDUs to {filename}")
        
    def save_to_parquet(self, filename="air_to_surface_pdus.parquet"):
        """Save the PDU data to a parquet file
        
        Note: Binary PDU data is stored as base64 encoded strings in the parquet file
        """
        import base64
        
        # Create a DataFrame with all fields except binary PDU data
        df_data = []
        for pdu in self.pdu_store:
            # Create a copy of the PDU data without the binary data
            pdu_copy = pdu.copy()
            # Convert binary data to base64 string
            pdu_copy['pdu_binary'] = base64.b64encode(pdu['pdu_binary']).decode('utf-8')
            # Convert position and velocity to separate columns
            if 'position' in pdu_copy:
                pdu_copy['position_x'] = pdu_copy['position'][0]
                pdu_copy['position_y'] = pdu_copy['position'][1]
                pdu_copy['position_z'] = pdu_copy['position'][2]
                del pdu_copy['position']
            if 'velocity' in pdu_copy:
                pdu_copy['velocity_x'] = pdu_copy['velocity'][0]
                pdu_copy['velocity_y'] = pdu_copy['velocity'][1]
                pdu_copy['velocity_z'] = pdu_copy['velocity'][2]
                del pdu_copy['velocity']
            if 'orientation' in pdu_copy:
                pdu_copy['orientation_heading'] = pdu_copy['orientation'][0]
                pdu_copy['orientation_pitch'] = pdu_copy['orientation'][1]
                pdu_copy['orientation_roll'] = pdu_copy['orientation'][2]
                del pdu_copy['orientation']
            
            df_data.append(pdu_copy)
        
        # Create DataFrame
        df = pd.DataFrame(df_data)
        
        # Save to parquet
        df.to_parquet(filename, index=False)
        print(f"Saved {len(self.pdu_store)} PDUs to {filename}")

def main():
    """Main function to demonstrate the PDU generator"""
    generator = AirToSurfacePDUGenerator()
    
    # Generate a bombing run scenario
    print("Generating bombing run scenario...")
    generator.generate_bombing_run_scenario(duration=180.0, num_aircraft=2, num_targets=5)
    
    # Save the generated PDUs
    generator.save_to_pickle("air_to_surface_bombing_run_pdus.pkl")
    generator.save_to_parquet("air_to_surface_bombing_run_pdus.parquet")
    
    # Generate a precision strike scenario
    print("\nGenerating precision strike scenario...")
    generator = AirToSurfacePDUGenerator()  # Create a new generator
    generator.generate_precision_strike_scenario(duration=120.0)
    
    # Save the generated PDUs
    generator.save_to_pickle("air_to_surface_precision_strike_pdus.pkl")
    generator.save_to_parquet("air_to_surface_precision_strike_pdus.parquet")
    
    print("\nAll air-to-surface scenarios complete.")

if __name__ == "__main__":
    main()