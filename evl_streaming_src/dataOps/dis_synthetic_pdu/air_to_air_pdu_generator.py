#!/usr/bin/python

"""
Air-to-Air Combat DIS PDU Generator

This script generates DIS PDUs for air-to-air combat scenarios.
It creates both Entity State PDUs for fighter aircraft and Fire/Detonation PDUs for missiles.

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
    F15 = EntityType(1, 2, 225, 1, 2, 0, 0)  # F-15 Eagle
    F22 = EntityType(1, 2, 225, 1, 11, 0, 0) # F-22 Raptor
    F35 = EntityType(1, 2, 225, 1, 3, 0, 0)  # F-35 Lightning II
    
    # Entity Kind = Platform (1), Domain = Air (2), Country = Russia (222)
    SU27 = EntityType(1, 2, 222, 1, 5, 0, 0)  # Su-27 Flanker
    MIG29 = EntityType(1, 2, 222, 1, 6, 0, 0) # MiG-29 Fulcrum
    SU35 = EntityType(1, 2, 222, 1, 13, 0, 0) # Su-35 Super Flanker
    
    @staticmethod
    def get_random_us_aircraft():
        """Get a random US aircraft type"""
        return random.choice([AircraftType.F16, AircraftType.F15, 
                              AircraftType.F22, AircraftType.F35])
    
    @staticmethod
    def get_random_russian_aircraft():
        """Get a random Russian aircraft type"""
        return random.choice([AircraftType.SU27, AircraftType.MIG29, 
                              AircraftType.SU35])

class MissileType:
    """Constants for missile types"""
    # Entity Kind = Munition (2), Domain = Anti-Air (5), Country = USA (225)
    AIM9 = EntityType(2, 5, 225, 1, 1, 0, 0)  # AIM-9 Sidewinder
    AIM120 = EntityType(2, 5, 225, 2, 1, 0, 0) # AIM-120 AMRAAM
    
    # Entity Kind = Munition (2), Domain = Anti-Air (5), Country = Russia (222)
    R73 = EntityType(2, 5, 222, 1, 1, 0, 0)   # R-73 (AA-11 Archer)
    R77 = EntityType(2, 5, 222, 2, 1, 0, 0)   # R-77 (AA-12 Adder)
    
    @staticmethod
    def get_random_us_missile():
        """Get a random US air-to-air missile type"""
        return random.choice([MissileType.AIM9, MissileType.AIM120])
    
    @staticmethod
    def get_random_russian_missile():
        """Get a random Russian air-to-air missile type"""
        return random.choice([MissileType.R73, MissileType.R77])

class Force:
    """Force ID constants"""
    FRIENDLY = 1  # Blue force
    OPPOSING = 2  # Red force
    NEUTRAL = 3   # White/neutral force

class AirToAirPDUGenerator:
    """Generator class for Air-to-Air Combat PDUs"""
    
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
            'scenario': 'air-to-air',
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
            'scenario': 'air-to-air',
            'pdu_binary': pdu_data
        })
        
        return entity_id
    
    def fire_missile(self, shooter_id, target_id, missile_type):
        """Fire a missile from an aircraft"""
        if shooter_id not in self.entities or not self.entities[shooter_id]['alive']:
            return None
            
        shooter = self.entities[shooter_id]
        
        # Create missile entity
        missile_entity_id = self.create_entity_id()
        event_id = self.create_event_id()
        
        # Create Fire PDU
        fire_pdu = FirePdu()
        fire_pdu.protocolVersion = 7
        fire_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        fire_pdu.firingEntityID = shooter['entity_id']
        
        # Set target entity (if known)
        if target_id in self.entities:
            fire_pdu.targetEntityID = self.entities[target_id]['entity_id']
        
        # Set event ID
        fire_pdu.eventID = event_id
        
        # Set munition ID
        fire_pdu.munitionExpendableID = missile_entity_id
        
        # Set fire mission index
        fire_pdu.fireMissionIndex = 0
        
        # Set location (same as shooter's position)
        fire_pdu.location = Vector3Double()
        fire_pdu.location.x = shooter['position'][0]
        fire_pdu.location.y = shooter['position'][1]
        fire_pdu.location.z = shooter['position'][2]
        
        # Set descriptor
        fire_pdu.descriptor = MunitionDescriptor()
        fire_pdu.descriptor.munitionType = missile_type
        fire_pdu.descriptor.warhead = 1000  # Conventional warhead
        fire_pdu.descriptor.fuse = 1000     # Proximity fuse
        fire_pdu.descriptor.quantity = 1
        fire_pdu.descriptor.rate = 0
        
        # Set velocity (initial missile velocity, typically shooter velocity + some delta)
        # Adding 200 m/s in the direction the shooter is facing
        heading = shooter['orientation'][0]
        velocity_boost = 200.0
        missile_velocity = [
            shooter['velocity'][0] + velocity_boost * math.cos(heading),
            shooter['velocity'][1] + velocity_boost * math.sin(heading),
            shooter['velocity'][2]
        ]
        
        fire_pdu.velocity = Vector3Float()
        fire_pdu.velocity.x = missile_velocity[0]
        fire_pdu.velocity.y = missile_velocity[1]
        fire_pdu.velocity.z = missile_velocity[2]
        
        # Set range (distance to target if known, otherwise a default value)
        if target_id in self.entities:
            target = self.entities[target_id]
            dx = target['position'][0] - shooter['position'][0]
            dy = target['position'][1] - shooter['position'][1]
            dz = target['position'][2] - shooter['position'][2]
            distance = math.sqrt(dx*dx + dy*dy + dz*dz)
            fire_pdu.range = distance
        else:
            fire_pdu.range = 10000.0  # Default range in meters
        
        # Create missile entity in our tracking system
        self.entities[missile_entity_id.entityID] = {
            'type': 'missile',
            'entity_type': missile_type,
            'entity_id': missile_entity_id,
            'force_id': shooter['force_id'],
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'orientation': shooter['orientation'].copy(),
            'shooter_id': shooter_id,
            'target_id': target_id if target_id in self.entities else None,
            'event_id': event_id,
            'alive': True,
            'flight_time': 0.0
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(fire_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'FirePdu',
            'entity_id': missile_entity_id.entityID,
            'force_id': shooter['force_id'],
            'shooter_id': shooter_id,
            'target_id': target_id if target_id in self.entities else None,
            'entity_kind': missile_type.entityKind,
            'entity_domain': missile_type.entityDomain,
            'entity_country': missile_type.entityCountry,
            'entity_category': missile_type.entityCategory,
            'entity_subcategory': missile_type.entitySubcategory,
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'scenario': 'air-to-air',
            'pdu_binary': pdu_data
        })
        
        # Create an initial EntityStatePdu for the missile
        self.update_missile(missile_entity_id.entityID)
        
        return missile_entity_id.entityID
    
    def update_missile(self, missile_id):
        """Update a missile's state"""
        if missile_id not in self.entities or not self.entities[missile_id]['alive']:
            return None
            
        missile = self.entities[missile_id]
        if missile['type'] != 'missile':
            return None
            
        # Create new Entity State PDU for the missile
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = missile['entity_id']
        espdu.forceId = missile['force_id']
        espdu.entityType = missile['entity_type']
        
        # Set position
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = missile['position'][0]
        espdu.entityLocation.y = missile['position'][1]
        espdu.entityLocation.z = missile['position'][2]
        
        # Set velocity
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = missile['velocity'][0]
        espdu.entityLinearVelocity.y = missile['velocity'][1]
        espdu.entityLinearVelocity.z = missile['velocity'][2]
        
        # Set orientation
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = missile['orientation'][0]
        espdu.entityOrientation.theta = missile['orientation'][1]
        espdu.entityOrientation.phi = missile['orientation'][2]
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"MSL{missile_id:02d}")
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': missile_id,
            'force_id': missile['force_id'],
            'entity_kind': missile['entity_type'].entityKind,
            'entity_domain': missile['entity_type'].entityDomain,
            'entity_country': missile['entity_type'].entityCountry,
            'entity_category': missile['entity_type'].entityCategory,
            'entity_subcategory': missile['entity_type'].entitySubcategory,
            'position': missile['position'].copy(),
            'velocity': missile['velocity'].copy(),
            'orientation': missile['orientation'].copy(),
            'scenario': 'air-to-air',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
    def detonate_missile(self, missile_id, detonation_result=0):
        """Detonate a missile"""
        if missile_id not in self.entities or not self.entities[missile_id]['alive']:
            return None
            
        missile = self.entities[missile_id]
        if missile['type'] != 'missile':
            return None
            
        # Create Detonation PDU
        detonation_pdu = DetonationPdu()
        detonation_pdu.protocolVersion = 7
        detonation_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        shooter_id = missile['shooter_id']
        if shooter_id in self.entities:
            detonation_pdu.firingEntityID = self.entities[shooter_id]['entity_id']
        
        # Set target entity (if known)
        target_id = missile['target_id']
        if target_id and target_id in self.entities:
            detonation_pdu.targetEntityID = self.entities[target_id]['entity_id']
        
        # Set munition ID
        detonation_pdu.munitionExpendableID = missile['entity_id']
        
        # Set event ID (same as the Fire PDU)
        detonation_pdu.eventID = missile['event_id']
        
        # Set location
        detonation_pdu.locationInWorldCoordinates = Vector3Double()
        detonation_pdu.locationInWorldCoordinates.x = missile['position'][0]
        detonation_pdu.locationInWorldCoordinates.y = missile['position'][1]
        detonation_pdu.locationInWorldCoordinates.z = missile['position'][2]
        
        # Set descriptor
        detonation_pdu.descriptor = MunitionDescriptor()
        detonation_pdu.descriptor.munitionType = missile['entity_type']
        detonation_pdu.descriptor.warhead = 1000  # Conventional warhead
        detonation_pdu.descriptor.fuse = 1000     # Proximity fuse
        detonation_pdu.descriptor.quantity = 1
        detonation_pdu.descriptor.rate = 0
        
        # Set velocity
        detonation_pdu.velocity = Vector3Float()
        detonation_pdu.velocity.x = missile['velocity'][0]
        detonation_pdu.velocity.y = missile['velocity'][1]
        detonation_pdu.velocity.z = missile['velocity'][2]
        
        # Set detonation result
        # 0=Other, 1=Entity Impact, 2=Entity Proximate Detonation, 3=Ground Impact, 4=Ground Proximate Detonation
        # 5=Detonation, 6=None/Dud, 7=HE hit, 8=HE proximate detonation
        detonation_pdu.detonationResult = detonation_result
        
        # Set location relative to entity (if detonation related to target entity)
        if detonation_result in [1, 2, 7, 8] and target_id and target_id in self.entities:
            target = self.entities[target_id]
            detonation_pdu.locationInEntityCoordinates = Vector3Float()
            detonation_pdu.locationInEntityCoordinates.x = missile['position'][0] - target['position'][0]
            detonation_pdu.locationInEntityCoordinates.y = missile['position'][1] - target['position'][1]
            detonation_pdu.locationInEntityCoordinates.z = missile['position'][2] - target['position'][2]
        else:
            detonation_pdu.locationInEntityCoordinates = Vector3Float()
            detonation_pdu.locationInEntityCoordinates.x = 0.0
            detonation_pdu.locationInEntityCoordinates.y = 0.0
            detonation_pdu.locationInEntityCoordinates.z = 0.0
            
        # Mark missile as no longer alive
        missile['alive'] = False
        
        # If detonation was a hit on target, mark target as destroyed (for some result types)
        if detonation_result in [1, 7] and target_id and target_id in self.entities:
            self.entities[target_id]['alive'] = False
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(detonation_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'DetonationPdu',
            'entity_id': missile_id,
            'force_id': missile['force_id'],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'entity_kind': missile['entity_type'].entityKind,
            'entity_domain': missile['entity_type'].entityDomain,
            'entity_country': missile['entity_type'].entityCountry,
            'entity_category': missile['entity_type'].entityCategory,
            'entity_subcategory': missile['entity_type'].entitySubcategory,
            'position': missile['position'].copy(),
            'velocity': missile['velocity'].copy(),
            'detonation_result': detonation_result,
            'scenario': 'air-to-air',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
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
                
            # Update entity position based on velocity
            new_position = [
                entity['position'][0] + entity['velocity'][0] * self.time_step,
                entity['position'][1] + entity['velocity'][1] * self.time_step,
                entity['position'][2] + entity['velocity'][2] * self.time_step
            ]
            entity['position'] = new_position
            
            # If entity is a missile, update its flight time
            if entity['type'] == 'missile':
                entity['flight_time'] += self.time_step
                
                # If missile has a target, adjust velocity to track target
                target_id = entity['target_id']
                if target_id and target_id in self.entities and self.entities[target_id]['alive']:
                    target = self.entities[target_id]
                    
                    # Calculate direction to target
                    dx = target['position'][0] - entity['position'][0]
                    dy = target['position'][1] - entity['position'][1]
                    dz = target['position'][2] - entity['position'][2]
                    distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                    
                    # If close enough, detonate
                    if distance < 50.0:  # Proximity detonation range
                        # 50% chance of hit, 50% chance of proximity detonation
                        result = 1 if random.random() < 0.5 else 2  # 1=Entity Impact, 2=Entity Proximate Detonation
                        self.detonate_missile(entity_id, result)
                        continue
                    
                    # Adjust missile velocity to track target (simplistic guidance)
                    if distance > 0.1:  # Avoid division by zero
                        speed = math.sqrt(entity['velocity'][0]**2 + entity['velocity'][1]**2 + entity['velocity'][2]**2)
                        
                        # Normalize direction vector
                        dx /= distance
                        dy /= distance
                        dz /= distance
                        
                        # Start with current velocity direction
                        current_direction = [entity['velocity'][0] / speed, 
                                            entity['velocity'][1] / speed, 
                                            entity['velocity'][2] / speed]
                        
                        # Blend current direction with target direction (limited turning)
                        # More aggressive tracking as time goes on
                        max_turn_rate = min(0.8, 0.2 + entity['flight_time'] * 0.1)  # Increases over time
                        blend_factor = min(max_turn_rate, 1.0)
                        
                        new_direction = [
                            (1.0 - blend_factor) * current_direction[0] + blend_factor * dx,
                            (1.0 - blend_factor) * current_direction[1] + blend_factor * dy,
                            (1.0 - blend_factor) * current_direction[2] + blend_factor * dz
                        ]
                        
                        # Normalize new direction
                        dir_magnitude = math.sqrt(new_direction[0]**2 + new_direction[1]**2 + new_direction[2]**2)
                        if dir_magnitude > 0.0001:
                            new_direction = [d / dir_magnitude for d in new_direction]
                            
                        # Increase speed slightly over time (acceleration)
                        new_speed = min(speed * 1.02, 1000.0)  # Maximum speed cap
                        
                        # Update velocity
                        entity['velocity'] = [
                            new_direction[0] * new_speed,
                            new_direction[1] * new_speed,
                            new_direction[2] * new_speed
                        ]
                        
                        # Update orientation based on velocity
                        heading = math.atan2(entity['velocity'][1], entity['velocity'][0])
                        pitch = math.asin(entity['velocity'][2] / new_speed) if new_speed > 0.0001 else 0
                        entity['orientation'] = [heading, pitch, 0.0]
                
                # Detonate missile if it's been flying too long (fuel exhaustion)
                if entity['flight_time'] > 60.0:  # 60 seconds max flight time
                    self.detonate_missile(entity_id, 6)  # 6=None/Dud
                    continue
                    
                # Generate Entity State PDU for the missile
                self.update_missile(entity_id)
            
            # For aircraft, generate an Entity State PDU periodically
            elif entity['type'] == 'aircraft' and self.current_time % 5.0 < self.time_step:
                self.update_aircraft(entity_id, entity['position'], entity['velocity'], entity['orientation'])
    
    def generate_dogfight_scenario(self, duration=120.0, num_blue=2, num_red=2):
        """Generate a complete air-to-air dogfight scenario
        
        Args:
            duration: Duration of the scenario in seconds
            num_blue: Number of blue (friendly) aircraft
            num_red: Number of red (opposing) aircraft
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create blue force aircraft
        blue_aircraft = []
        for i in range(num_blue):
            # Position blue aircraft in west side of battle area
            position = [-10000.0 + random.uniform(-2000, 2000), 
                        random.uniform(-5000, 5000), 
                        5000.0 + random.uniform(-500, 500)]
            
            # Initial velocity moving east
            velocity = [300.0 + random.uniform(-20, 20), 
                        random.uniform(-10, 10), 
                        random.uniform(-5, 5)]
            
            # Randomly select aircraft type
            aircraft_types = [AircraftType.F16, AircraftType.F15, AircraftType.F22, AircraftType.F35]
            aircraft_type = random.choice(aircraft_types)
            
            # Create the aircraft
            entity_id = self.create_aircraft(aircraft_type, Force.FRIENDLY, position, velocity)
            blue_aircraft.append(entity_id)
        
        # Create red force aircraft
        red_aircraft = []
        for i in range(num_red):
            # Position red aircraft in east side of battle area
            position = [10000.0 + random.uniform(-2000, 2000), 
                        random.uniform(-5000, 5000), 
                        5000.0 + random.uniform(-500, 500)]
            
            # Initial velocity moving west
            velocity = [-300.0 + random.uniform(-20, 20), 
                        random.uniform(-10, 10), 
                        random.uniform(-5, 5)]
            
            # Randomly select aircraft type
            aircraft_types = [AircraftType.SU27, AircraftType.MIG29, AircraftType.SU35]
            aircraft_type = random.choice(aircraft_types)
            
            # Create the aircraft
            entity_id = self.create_aircraft(aircraft_type, Force.OPPOSING, position, velocity)
            red_aircraft.append(entity_id)
        
        # Run the simulation for the specified duration
        engagement_started = False
        
        while self.current_time < duration:
            # Update the simulation
            self.update_simulation()
            
            # Check if aircraft are within missile range of each other
            if not engagement_started and self.current_time > 20.0:
                blue_alive = [a for a in blue_aircraft if a in self.entities and self.entities[a]['alive']]
                red_alive = [a for a in red_aircraft if a in self.entities and self.entities[a]['alive']]
                
                min_distance = float('inf')
                for blue_id in blue_alive:
                    blue = self.entities[blue_id]
                    for red_id in red_alive:
                        red = self.entities[red_id]
                        dx = red['position'][0] - blue['position'][0]
                        dy = red['position'][1] - blue['position'][1]
                        dz = red['position'][2] - blue['position'][2]
                        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                        min_distance = min(min_distance, distance)
                
                # When aircraft are within 15km of each other, start engagement
                if min_distance < 15000:
                    engagement_started = True
                    
                    # Blue force fires first
                    for blue_id in blue_alive:
                        if red_alive:
                            target_id = random.choice(red_alive)
                            missile_type = random.choice([MissileType.AIM9, MissileType.AIM120])
                            self.fire_missile(blue_id, target_id, missile_type)
                    
                    # Wait 5 seconds before red force responds
                    for _ in range(5):
                        self.update_simulation()
                    
                    # Red force fires back
                    red_alive = [a for a in red_aircraft if a in self.entities and self.entities[a]['alive']]
                    blue_alive = [a for a in blue_aircraft if a in self.entities and self.entities[a]['alive']]
                    
                    for red_id in red_alive:
                        if blue_alive:
                            target_id = random.choice(blue_alive)
                            missile_type = random.choice([MissileType.R73, MissileType.R77])
                            self.fire_missile(red_id, target_id, missile_type)
            
            # Every 30 seconds, if there are still aircraft alive on both sides, fire more missiles
            if engagement_started and self.current_time % 30.0 < self.time_step:
                blue_alive = [a for a in blue_aircraft if a in self.entities and self.entities[a]['alive']]
                red_alive = [a for a in red_aircraft if a in self.entities and self.entities[a]['alive']]
                
                if blue_alive and red_alive:
                    # Each aircraft on the blue side might fire a missile
                    for blue_id in blue_alive:
                        if random.random() < 0.7 and red_alive:  # 70% chance to fire
                            target_id = random.choice(red_alive)
                            missile_type = random.choice([MissileType.AIM9, MissileType.AIM120])
                            self.fire_missile(blue_id, target_id, missile_type)
                    
                    # Wait 2-5 seconds before red force responds
                    wait_time = random.randint(2, 5)
                    for _ in range(wait_time):
                        self.update_simulation()
                    
                    # Update aircraft lists after the wait
                    blue_alive = [a for a in blue_aircraft if a in self.entities and self.entities[a]['alive']]
                    red_alive = [a for a in red_aircraft if a in self.entities and self.entities[a]['alive']]
                    
                    # Each aircraft on the red side might fire a missile
                    for red_id in red_alive:
                        if random.random() < 0.7 and blue_alive:  # 70% chance to fire
                            target_id = random.choice(blue_alive)
                            missile_type = random.choice([MissileType.R73, MissileType.R77])
                            self.fire_missile(red_id, target_id, missile_type)
            
            # If all aircraft on one side are destroyed, end the scenario
            blue_alive = [a for a in blue_aircraft if a in self.entities and self.entities[a]['alive']]
            red_alive = [a for a in red_aircraft if a in self.entities and self.entities[a]['alive']]
            
            if not blue_alive or not red_alive:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
                
        print(f"Scenario completed at time {self.current_time:.1f} seconds")
        print(f"Blue aircraft alive: {len(blue_alive)}/{num_blue}")
        print(f"Red aircraft alive: {len(red_alive)}/{num_red}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def generate_intercept_scenario(self, duration=120.0):
        """Generate an air intercept scenario with a single fighter intercepting a target
        
        Args:
            duration: Duration of the scenario in seconds
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create interceptor (blue force)
        interceptor_position = [-20000.0, 0.0, 8000.0]
        interceptor_velocity = [400.0, 0.0, 0.0]
        interceptor_id = self.create_aircraft(AircraftType.F22, Force.FRIENDLY, interceptor_position, interceptor_velocity)
        
        # Create target (red force)
        target_position = [20000.0, 2000.0, 10000.0]
        target_velocity = [-200.0, 0.0, 0.0]
        target_id = self.create_aircraft(AircraftType.SU27, Force.OPPOSING, target_position, target_velocity)
        
        # Run the simulation for the specified duration
        missile_fired = False
        
        while self.current_time < duration:
            # Update the simulation
            self.update_simulation()
            
            # When interceptor is within missile range, fire a missile
            if not missile_fired and self.current_time > 5.0:
                interceptor = self.entities[interceptor_id]
                target = self.entities[target_id]
                
                dx = target['position'][0] - interceptor['position'][0]
                dy = target['position'][1] - interceptor['position'][1]
                dz = target['position'][2] - interceptor['position'][2]
                distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                
                # Fire when within 20km
                if distance < 20000.0:
                    missile_fired = True
                    self.fire_missile(interceptor_id, target_id, MissileType.AIM120)
            
            # If target is destroyed, end the scenario
            if target_id not in self.entities or not self.entities[target_id]['alive']:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Intercept scenario completed at time {self.current_time:.1f} seconds")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def save_to_pickle(self, filename="air_to_air_pdus.pkl"):
        """Save the PDU data to a pickle file"""
        with open(filename, 'wb') as f:
            pickle.dump(self.pdu_store, f)
        print(f"Saved {len(self.pdu_store)} PDUs to {filename}")
        
    def save_to_parquet(self, filename="air_to_air_pdus.parquet"):
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
    generator = AirToAirPDUGenerator()
    
    # Generate a dogfight scenario
    print("Generating dogfight scenario...")
    generator.generate_dogfight_scenario(duration=120.0, num_blue=2, num_red=2)
    
    # Save the generated PDUs
    generator.save_to_pickle("air_to_air_dogfight_pdus.pkl")
    generator.save_to_parquet("air_to_air_dogfight_pdus.parquet")
    
    # Generate an intercept scenario
    print("\nGenerating intercept scenario...")
    generator = AirToAirPDUGenerator()  # Create a new generator
    generator.generate_intercept_scenario(duration=120.0)
    
    # Save the generated PDUs
    generator.save_to_pickle("air_to_air_intercept_pdus.pkl")
    generator.save_to_parquet("air_to_air_intercept_pdus.parquet")
    
    print("\nAll air-to-air scenarios complete.")

if __name__ == "__main__":
    main()