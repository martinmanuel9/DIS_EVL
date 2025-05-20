#!/usr/bin/python

"""
Surface-to-Surface Combat DIS PDU Generator

This script generates DIS PDUs for surface-to-surface combat scenarios.
It creates Entity State PDUs for ships and land platforms, as well as Fire/Detonation PDUs for missiles.

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

# Import utility functions from the utils module
# Note: Ensure dis_pdu_utils.py is in the same directory or in the Python path
from dis_pdu_utils import *

class SurfaceToSurfacePDUGenerator:
    """Generator class for Surface-to-Surface Combat PDUs"""
    
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
    
    def serialize_pdu(self, pdu):
        """Serialize a PDU to binary format"""
        memory_stream = BytesIO()
        output_stream = DataOutputStream(memory_stream)
        pdu.serialize(output_stream)
        return memory_stream.getvalue()
    
    def create_ship(self, ship_type, force_id, position, velocity, orientation=None):
        """Create a ship entity"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the ship
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = ship_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]  # Usually close to 0 for ships
        
        # Set velocity (X, Y, Z in meters/sec)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = velocity[0]
        espdu.entityLinearVelocity.y = velocity[1]
        espdu.entityLinearVelocity.z = velocity[2]  # Usually 0 for ships
        
        # Set orientation (Psi, Theta, Phi in radians)
        if orientation is None:
            # Calculate orientation from velocity vector for ships (heading/yaw only)
            heading = math.atan2(velocity[1], velocity[0]) if any(velocity) else 0
            orientation = [heading, 0.0, 0.0]  # No pitch or roll for ships in simple simulation
        
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = orientation[0]    # Heading/Yaw
        espdu.entityOrientation.theta = orientation[1]  # Pitch (usually 0 for ships)
        espdu.entityOrientation.phi = orientation[2]    # Roll (usually 0 for ships)
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"SHIP{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'ship',
            'entity_type': ship_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'alive': True,
            'last_fire_time': 0,
            'detection_range': 100000.0,  # 100 km typical ship radar detection range
            'missile_engagement_range': 70000.0,  # 70 km typical anti-ship missile range
            'damage_level': 0,  # 0=None, 1=Slight, 2=Moderate, 3=Destroyed
            'max_speed': 15.0  # m/s, approximately 30 knots
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': ship_type.entityKind,
            'entity_domain': ship_type.entityDomain,
            'entity_country': ship_type.entityCountry,
            'entity_category': ship_type.entityCategory,
            'entity_subcategory': ship_type.entitySubcategory,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def create_land_vehicle(self, vehicle_type, force_id, position, velocity, orientation=None):
        """Create a land vehicle entity (e.g., tank, IFV)"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the vehicle
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = vehicle_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]  # Z is altitude (terrain height)
        
        # Set velocity (X, Y, Z in meters/sec)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = velocity[0]
        espdu.entityLinearVelocity.y = velocity[1]
        espdu.entityLinearVelocity.z = velocity[2]  # Usually 0 for ground vehicles
        
        # Set orientation (Psi, Theta, Phi in radians)
        if orientation is None:
            # Calculate orientation from velocity vector
            heading = math.atan2(velocity[1], velocity[0]) if any(velocity) else 0
            orientation = [heading, 0.0, 0.0]  # No pitch or roll in simple simulation
        
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = orientation[0]    # Heading/Yaw
        espdu.entityOrientation.theta = orientation[1]  # Pitch (usually slight for terrain)
        espdu.entityOrientation.phi = orientation[2]    # Roll (usually slight for terrain)
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"LAND{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'land_vehicle',
            'entity_type': vehicle_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'alive': True,
            'last_fire_time': 0,
            'detection_range': 5000.0,  # 5 km typical land vehicle detection range
            'missile_engagement_range': 4000.0,  # 4 km typical anti-tank missile range
            'damage_level': 0,  # 0=None, 1=Slight, 2=Moderate, 3=Destroyed
            'max_speed': 20.0  # m/s, approximately 72 km/h
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': vehicle_type.entityKind,
            'entity_domain': vehicle_type.entityDomain,
            'entity_country': vehicle_type.entityCountry,
            'entity_category': vehicle_type.entityCategory,
            'entity_subcategory': vehicle_type.entitySubcategory,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def update_entity(self, entity_id, new_position=None, new_velocity=None, new_orientation=None, new_appearance=None):
        """Update an entity's state"""
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
        
        # Update position if provided, otherwise use current position
        espdu.entityLocation = Vector3Double()
        if new_position:
            espdu.entityLocation.x = new_position[0]
            espdu.entityLocation.y = new_position[1]
            espdu.entityLocation.z = new_position[2]
            entity['position'] = new_position.copy()
        else:
            espdu.entityLocation.x = entity['position'][0]
            espdu.entityLocation.y = entity['position'][1]
            espdu.entityLocation.z = entity['position'][2]
        
        # Update velocity if provided, otherwise use current velocity
        espdu.entityLinearVelocity = Vector3Float()
        if new_velocity:
            espdu.entityLinearVelocity.x = new_velocity[0]
            espdu.entityLinearVelocity.y = new_velocity[1]
            espdu.entityLinearVelocity.z = new_velocity[2]
            entity['velocity'] = new_velocity.copy()
        else:
            espdu.entityLinearVelocity.x = entity['velocity'][0]
            espdu.entityLinearVelocity.y = entity['velocity'][1]
            espdu.entityLinearVelocity.z = entity['velocity'][2]
        
        # Update orientation if provided, otherwise calculate from velocity or use current
        if new_orientation:
            heading, pitch, roll = new_orientation
            entity['orientation'] = new_orientation.copy()
        elif new_velocity:
            # Calculate heading from velocity for surface entities
            heading = math.atan2(new_velocity[1], new_velocity[0]) if any(new_velocity) else entity['orientation'][0]
            pitch = 0.0  # Surface entities typically have zero pitch
            roll = 0.0   # Surface entities typically have zero roll
            entity['orientation'] = [heading, pitch, roll]
        else:
            heading, pitch, roll = entity['orientation']
            
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = heading
        espdu.entityOrientation.theta = pitch
        espdu.entityOrientation.phi = roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        if new_appearance is not None:
            espdu.entityAppearance = new_appearance
        else:
            espdu.entityAppearance = 0  # Standard appearance
        
        # Set marking
        espdu.marking = Marking()
        if entity['type'] == 'ship':
            espdu.marking.setMarkingString(f"SHIP{entity_id:02d}")
        else:
            espdu.marking.setMarkingString(f"LAND{entity_id:02d}")
        
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
            'position': entity['position'].copy(),
            'velocity': entity['velocity'].copy(),
            'orientation': entity['orientation'].copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id
    
    def fire_missile(self, shooter_id, target_id, missile_type):
        """Fire a missile from a ship or land vehicle"""
        if shooter_id not in self.entities or not self.entities[shooter_id]['alive']:
            return None
            
        if target_id not in self.entities or not self.entities[target_id]['alive']:
            return None
            
        shooter = self.entities[shooter_id]
        target = self.entities[target_id]
        
        # Create missile entity
        missile_entity_id = self.create_entity_id()
        event_id = self.create_event_id()
        
        # Create Fire PDU
        fire_pdu = FirePdu()
        fire_pdu.protocolVersion = 7
        fire_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        fire_pdu.firingEntityID = shooter['entity_id']
        
        # Set target entity
        fire_pdu.targetEntityID = target['entity_id']
        
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
        
        # Calculate initial missile direction towards target
        dx = target['position'][0] - shooter['position'][0]
        dy = target['position'][1] - shooter['position'][1]
        dz = target['position'][2] - shooter['position'][2]
        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
        
        # Normalize direction
        if distance > 0:
            dx /= distance
            dy /= distance
            dz /= distance
        
        # Set initial missile velocity 
        initial_speed = 200.0  # m/s initial speed for surface missiles
        
        # Lead the target by estimating where it will be
        lead_time = distance / 400.0  # Assume average missile speed of 400 m/s
        
        # Estimated target position
        target_future_pos = [
            target['position'][0] + target['velocity'][0] * lead_time,
            target['position'][1] + target['velocity'][1] * lead_time,
            target['position'][2] + target['velocity'][2] * lead_time
        ]
        
        # Direction to future position
        dx_lead = target_future_pos[0] - shooter['position'][0]
        dy_lead = target_future_pos[1] - shooter['position'][1]
        dz_lead = target_future_pos[2] - shooter['position'][2]
        distance_lead = math.sqrt(dx_lead*dx_lead + dy_lead*dy_lead + dz_lead*dz_lead)
        
        if distance_lead > 0:
            dx_lead /= distance_lead
            dy_lead /= distance_lead
            dz_lead /= distance_lead
        
        # Mix current and lead direction
        mix_factor = 0.8  # 80% lead, 20% direct
        dx_final = (1.0 - mix_factor) * dx + mix_factor * dx_lead
        dy_final = (1.0 - mix_factor) * dy + mix_factor * dy_lead
        dz_final = (1.0 - mix_factor) * dz + mix_factor * dz_lead
        
        # Normalize final direction
        dir_magnitude = math.sqrt(dx_final*dx_final + dy_final*dy_final + dz_final*dz_final)
        if dir_magnitude > 0:
            dx_final /= dir_magnitude
            dy_final /= dir_magnitude
            dz_final /= dir_magnitude
        
        missile_velocity = [
            dx_final * initial_speed,
            dy_final * initial_speed,
            dz_final * initial_speed
        ]
        
        fire_pdu.velocity = Vector3Float()
        fire_pdu.velocity.x = missile_velocity[0]
        fire_pdu.velocity.y = missile_velocity[1]
        fire_pdu.velocity.z = missile_velocity[2]
        
        # Set range (distance to target)
        fire_pdu.range = distance
        
        # Create missile entity in our tracking system
        self.entities[missile_entity_id.entityID] = {
            'type': 'missile',
            'entity_type': missile_type,
            'entity_id': missile_entity_id,
            'force_id': shooter['force_id'],
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'orientation': [math.atan2(dy_final, dx_final), math.asin(dz_final) if abs(dz_final) < 1.0 else 0.0, 0.0],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'event_id': event_id,
            'alive': True,
            'flight_time': 0.0,
            'max_speed': 500.0,  # m/s, typical anti-ship missile max speed
            'acceleration': 50.0  # m/s^2, typical missile acceleration
        }
        
        # Update the shooter's last fire time
        shooter['last_fire_time'] = self.current_time
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(fire_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'FirePdu',
            'entity_id': missile_entity_id.entityID,
            'force_id': shooter['force_id'],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'entity_kind': missile_type.entityKind,
            'entity_domain': missile_type.entityDomain,
            'entity_country': missile_type.entityCountry,
            'entity_category': missile_type.entityCategory,
            'entity_subcategory': missile_type.entitySubcategory,
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        # Create an initial EntityStatePdu for the missile
        self.update_entity(missile_entity_id.entityID)
        
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
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
    def detonate_missile(self, missile_id, detonation_result=DetonationResult.ENTITY_IMPACT):
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
        
        # Set target entity
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
        detonation_pdu.detonationResult = detonation_result
        
        # Set location relative to entity (if detonation related to target entity)
        if detonation_result in [DetonationResult.ENTITY_IMPACT, DetonationResult.ENTITY_PROXIMATE_DETONATION] and target_id and target_id in self.entities:
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
        
        # If detonation was a hit on target, increase its damage level
        if detonation_result == DetonationResult.ENTITY_IMPACT and target_id and target_id in self.entities:
            target = self.entities[target_id]
            target['damage_level'] += 1
            
            # If damage level is high enough, mark as destroyed
            if target['damage_level'] >= 3:
                target['alive'] = False
        
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
            'position': missile['position'].copy
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def create_land_vehicle(self, vehicle_type, force_id, position, velocity, orientation=None):
        """Create a land vehicle entity (e.g., tank, IFV)"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the vehicle
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = vehicle_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]  # Z is altitude (terrain height)
        
        # Set velocity (X, Y, Z in meters/sec)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = velocity[0]
        espdu.entityLinearVelocity.y = velocity[1]
        espdu.entityLinearVelocity.z = velocity[2]  # Usually 0 for ground vehicles
        
        # Set orientation (Psi, Theta, Phi in radians)
        if orientation is None:
            # Calculate orientation from velocity vector
            heading = math.atan2(velocity[1], velocity[0]) if any(velocity) else 0
            orientation = [heading, 0.0, 0.0]  # No pitch or roll in simple simulation
        
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = orientation[0]    # Heading/Yaw
        espdu.entityOrientation.theta = orientation[1]  # Pitch (usually slight for terrain)
        espdu.entityOrientation.phi = orientation[2]    # Roll (usually slight for terrain)
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"LAND{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'land_vehicle',
            'entity_type': vehicle_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'alive': True,
            'last_fire_time': 0,
            'detection_range': 5000.0,  # 5 km typical land vehicle detection range
            'missile_engagement_range': 4000.0,  # 4 km typical anti-tank missile range
            'damage_level': 0,  # 0=None, 1=Slight, 2=Moderate, 3=Destroyed
            'max_speed': 20.0  # m/s, approximately 72 km/h
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': vehicle_type.entityKind,
            'entity_domain': vehicle_type.entityDomain,
            'entity_country': vehicle_type.entityCountry,
            'entity_category': vehicle_type.entityCategory,
            'entity_subcategory': vehicle_type.entitySubcategory,
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def update_entity(self, entity_id, new_position=None, new_velocity=None, new_orientation=None, new_appearance=None):
        """Update an entity's state"""
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
        
        # Update position if provided, otherwise use current position
        espdu.entityLocation = Vector3Double()
        if new_position:
            espdu.entityLocation.x = new_position[0]
            espdu.entityLocation.y = new_position[1]
            espdu.entityLocation.z = new_position[2]
            entity['position'] = new_position.copy()
        else:
            espdu.entityLocation.x = entity['position'][0]
            espdu.entityLocation.y = entity['position'][1]
            espdu.entityLocation.z = entity['position'][2]
        
        # Update velocity if provided, otherwise use current velocity
        espdu.entityLinearVelocity = Vector3Float()
        if new_velocity:
            espdu.entityLinearVelocity.x = new_velocity[0]
            espdu.entityLinearVelocity.y = new_velocity[1]
            espdu.entityLinearVelocity.z = new_velocity[2]
            entity['velocity'] = new_velocity.copy()
        else:
            espdu.entityLinearVelocity.x = entity['velocity'][0]
            espdu.entityLinearVelocity.y = entity['velocity'][1]
            espdu.entityLinearVelocity.z = entity['velocity'][2]
        
        # Update orientation if provided, otherwise calculate from velocity or use current
        if new_orientation:
            heading, pitch, roll = new_orientation
            entity['orientation'] = new_orientation.copy()
        elif new_velocity:
            # Calculate heading from velocity for surface entities
            heading = math.atan2(new_velocity[1], new_velocity[0]) if any(new_velocity) else entity['orientation'][0]
            pitch = 0.0  # Surface entities typically have zero pitch
            roll = 0.0   # Surface entities typically have zero roll
            entity['orientation'] = [heading, pitch, roll]
        else:
            heading, pitch, roll = entity['orientation']
            
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = heading
        espdu.entityOrientation.theta = pitch
        espdu.entityOrientation.phi = roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 2  # DRM_RPW: Rate Position Orientation
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        if new_appearance is not None:
            espdu.entityAppearance = new_appearance
        else:
            espdu.entityAppearance = 0  # Standard appearance
        
        # Set marking
        espdu.marking = Marking()
        if entity['type'] == 'ship':
            espdu.marking.setMarkingString(f"SHIP{entity_id:02d}")
        else:
            espdu.marking.setMarkingString(f"LAND{entity_id:02d}")
        
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
            'position': entity['position'].copy(),
            'velocity': entity['velocity'].copy(),
            'orientation': entity['orientation'].copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return entity_id
    
    def fire_missile(self, shooter_id, target_id, missile_type):
        """Fire a missile from a ship or land vehicle"""
        if shooter_id not in self.entities or not self.entities[shooter_id]['alive']:
            return None
            
        if target_id not in self.entities or not self.entities[target_id]['alive']:
            return None
            
        shooter = self.entities[shooter_id]
        target = self.entities[target_id]
        
        # Create missile entity
        missile_entity_id = self.create_entity_id()
        event_id = self.create_event_id()
        
        # Create Fire PDU
        fire_pdu = FirePdu()
        fire_pdu.protocolVersion = 7
        fire_pdu.exerciseID = self.exercise_id
        
        # Set firing entity
        fire_pdu.firingEntityID = shooter['entity_id']
        
        # Set target entity
        fire_pdu.targetEntityID = target['entity_id']
        
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
        
        # Calculate initial missile direction towards target
        dx = target['position'][0] - shooter['position'][0]
        dy = target['position'][1] - shooter['position'][1]
        dz = target['position'][2] - shooter['position'][2]
        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
        
        # Normalize direction
        if distance > 0:
            dx /= distance
            dy /= distance
            dz /= distance
        
        # Set initial missile velocity 
        initial_speed = 200.0  # m/s initial speed for surface missiles
        
        # Lead the target by estimating where it will be
        lead_time = distance / 400.0  # Assume average missile speed of 400 m/s
        
        # Estimated target position
        target_future_pos = [
            target['position'][0] + target['velocity'][0] * lead_time,
            target['position'][1] + target['velocity'][1] * lead_time,
            target['position'][2] + target['velocity'][2] * lead_time
        ]
        
        # Direction to future position
        dx_lead = target_future_pos[0] - shooter['position'][0]
        dy_lead = target_future_pos[1] - shooter['position'][1]
        dz_lead = target_future_pos[2] - shooter['position'][2]
        distance_lead = math.sqrt(dx_lead*dx_lead + dy_lead*dy_lead + dz_lead*dz_lead)
        
        if distance_lead > 0:
            dx_lead /= distance_lead
            dy_lead /= distance_lead
            dz_lead /= distance_lead
        
        # Mix current and lead direction
        mix_factor = 0.8  # 80% lead, 20% direct
        dx_final = (1.0 - mix_factor) * dx + mix_factor * dx_lead
        dy_final = (1.0 - mix_factor) * dy + mix_factor * dy_lead
        dz_final = (1.0 - mix_factor) * dz + mix_factor * dz_lead
        
        # Normalize final direction
        dir_magnitude = math.sqrt(dx_final*dx_final + dy_final*dy_final + dz_final*dz_final)
        if dir_magnitude > 0:
            dx_final /= dir_magnitude
            dy_final /= dir_magnitude
            dz_final /= dir_magnitude
        
        missile_velocity = [
            dx_final * initial_speed,
            dy_final * initial_speed,
            dz_final * initial_speed
        ]
        
        fire_pdu.velocity = Vector3Float()
        fire_pdu.velocity.x = missile_velocity[0]
        fire_pdu.velocity.y = missile_velocity[1]
        fire_pdu.velocity.z = missile_velocity[2]
        
        # Set range (distance to target)
        fire_pdu.range = distance
        
        # Create missile entity in our tracking system
        self.entities[missile_entity_id.entityID] = {
            'type': 'missile',
            'entity_type': missile_type,
            'entity_id': missile_entity_id,
            'force_id': shooter['force_id'],
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'orientation': [math.atan2(dy_final, dx_final), math.asin(dz_final) if abs(dz_final) < 1.0 else 0.0, 0.0],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'event_id': event_id,
            'alive': True,
            'flight_time': 0.0,
            'max_speed': 500.0,  # m/s, typical anti-ship missile max speed
            'acceleration': 50.0  # m/s^2, typical missile acceleration
        }
        
        # Update the shooter's last fire time
        shooter['last_fire_time'] = self.current_time
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(fire_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'FirePdu',
            'entity_id': missile_entity_id.entityID,
            'force_id': shooter['force_id'],
            'shooter_id': shooter_id,
            'target_id': target_id,
            'entity_kind': missile_type.entityKind,
            'entity_domain': missile_type.entityDomain,
            'entity_country': missile_type.entityCountry,
            'entity_category': missile_type.entityCategory,
            'entity_subcategory': missile_type.entitySubcategory,
            'position': shooter['position'].copy(),
            'velocity': missile_velocity.copy(),
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        # Create an initial EntityStatePdu for the missile
        self.update_entity(missile_entity_id.entityID)
        
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
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
    def detonate_missile(self, missile_id, detonation_result=DetonationResult.ENTITY_IMPACT):
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
        
        # Set target entity
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
        detonation_pdu.detonationResult = detonation_result
        
        # Set location relative to entity (if detonation related to target entity)
        if detonation_result in [DetonationResult.ENTITY_IMPACT, DetonationResult.ENTITY_PROXIMATE_DETONATION] and target_id and target_id in self.entities:
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
        
        # If detonation was a hit on target, increase its damage level
        if detonation_result == DetonationResult.ENTITY_IMPACT and target_id and target_id in self.entities:
            target = self.entities[target_id]
            target['damage_level'] += 1
            
            # If damage level is high enough, mark as destroyed
            if target['damage_level'] >= 3:
                target['alive'] = False
        
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
            'scenario': 'surface-to-surface',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
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
            
            # If entity is a missile, update its flight time and guidance
            if entity['type'] == 'missile':
                entity['flight_time'] += self.time_step
                
                # Accelerate missile (up to max speed)
                current_speed = math.sqrt(entity['velocity'][0]**2 + entity['velocity'][1]**2 + entity['velocity'][2]**2)
                new_speed = min(current_speed + entity['acceleration'] * self.time_step, entity['max_speed'])
                
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
                    if distance < 50.0:  # Proximity detonation range (50m)
                        # Determine detonation result (hit or miss)
                        # Higher probability of hit for sea-skimming anti-ship missiles
                        hit_probability = 0.7  # 70% base hit probability
                        
                        # Modern anti-ship missiles have higher hit rates
                        if entity['entity_type'].entityCountry == CountryCodes.USA:
                            hit_probability = 0.8  # 80% for US systems
                            
                        result = DetonationResult.ENTITY_IMPACT if random.random() < hit_probability else DetonationResult.ENTITY_PROXIMATE_DETONATION
                        self.detonate_missile(entity_id, result)
                        continue
                    
                    # Adjust missile velocity to track target (guidance)
                    if distance > 0.1:  # Avoid division by zero
                        # Lead the target based on its velocity
                        lead_time = distance / new_speed  # Time to intercept
                        
                        # Estimated target position with lead
                        target_future_pos = [
                            target['position'][0] + target['velocity'][0] * lead_time,
                            target['position'][1] + target['velocity'][1] * lead_time,
                            target['position'][2] + target['velocity'][2] * lead_time
                        ]
                        
                        # Direction to future position
                        dx_lead = target_future_pos[0] - entity['position'][0]
                        dy_lead = target_future_pos[1] - entity['position'][1]
                        dz_lead = target_future_pos[2] - entity['position'][2]
                        distance_lead = math.sqrt(dx_lead*dx_lead + dy_lead*dy_lead + dz_lead*dz_lead)
                        
                        if distance_lead > 0:
                            dx_lead /= distance_lead
                            dy_lead /= distance_lead
                            dz_lead /= distance_lead
                        
                        # Normalize current velocity direction
                        current_direction = [
                            entity['velocity'][0] / current_speed,
                            entity['velocity'][1] / current_speed,
                            entity['velocity'][2] / current_speed
                        ]
                        
                        # Blend current direction with target direction (limited turning)
                        # More aggressive tracking as time goes on
                        max_turn_rate = min(0.8, 0.2 + entity['flight_time'] * 0.1)  # Increases over time
                        
                        # Calculate new direction by blending
                        new_direction = [
                            (1.0 - max_turn_rate) * current_direction[0] + max_turn_rate * dx_lead,
                            (1.0 - max_turn_rate) * current_direction[1] + max_turn_rate * dy_lead,
                            (1.0 - max_turn_rate) * current_direction[2] + max_turn_rate * dz_lead
                        ]
                        
                        # Normalize new direction
                        dir_magnitude = math.sqrt(new_direction[0]**2 + new_direction[1]**2 + new_direction[2]**2)
                        if dir_magnitude > 0.0001:
                            new_direction = [d / dir_magnitude for d in new_direction]
                        
                        # Update velocity with new direction and speed
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
                if entity['flight_time'] > 300.0:  # 5 minutes max flight time for anti-ship missiles
                    self.detonate_missile(entity_id, DetonationResult.NONE_DUD)
                    continue
                
                # Generate Entity State PDU for the missile
                self.update_missile(entity_id)
                
            # For ships and land vehicles, only update periodically to reduce PDU traffic
            elif self.current_time % 5.0 < self.time_step:
                self.update_entity(entity_id)
                
                # Check if this entity should engage targets
                if entity['type'] in ['ship', 'land_vehicle'] and self.current_time - entity['last_fire_time'] > 30.0:  # 30 seconds between launches
                    # Look for targets in range
                    for target_id, target in self.entities.items():
                        if not target['alive'] or target['force_id'] == entity['force_id'] or target['type'] in ['missile']:
                            continue
                            
                        # Calculate distance to target
                        dx = target['position'][0] - entity['position'][0]
                        dy = target['position'][1] - entity['position'][1]
                        dz = target['position'][2] - entity['position'][2]
                        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                        
                        # Check if target is within engagement range
                        if distance <= entity['missile_engagement_range']:
                            # Probability to fire depends on entity type and target range
                            fire_chance = 0.5  # Base 50% chance
                            
                            # Reduce chance for longer ranges
                            range_factor = 1.0 - (distance / entity['missile_engagement_range'])
                            fire_chance *= range_factor
                            
                            if random.random() < fire_chance:
                                # Select appropriate missile type for the target
                                if entity['type'] == 'ship' and target['type'] == 'ship':
                                    # Ship to ship engagement
                                    if entity['entity_type'].entityCountry == CountryCodes.USA:
                                        missile_type = CommonEntityTypes.HARPOON
                                    else:
                                        missile_type = CommonEntityTypes.SS_N_19
                                elif entity['type'] == 'land_vehicle' and target['type'] == 'land_vehicle':
                                    # Land to land engagement
                                    if entity['entity_type'].entityCountry == CountryCodes.USA:
                                        missile_type = CommonEntityTypes.AGM65
                                    else:
                                        missile_type = CommonEntityTypes.KH29
                                else:
                                    # Default missile type
                                    if entity['entity_type'].entityCountry == CountryCodes.USA:
                                        missile_type = CommonEntityTypes.HARPOON
                                    else:
                                        missile_type = CommonEntityTypes.SS_N_19
                                
                                self.fire_missile(entity_id, target_id, missile_type)
                                break  # Only fire at one target at a time
    
    def generate_naval_battle_scenario(self, duration=300.0, num_blue_ships=2, num_red_ships=2):
        """Generate a naval battle scenario with multiple ships engaging each other
        
        Args:
            duration: Duration of the scenario in seconds
            num_blue_ships: Number of blue force ships
            num_red_ships: Number of red force ships
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create blue force ships
        blue_ships = []
        for i in range(num_blue_ships):
            # US ships (west side)
            if i % 2 == 0:
                ship_type = CommonEntityTypes.DDG  # Destroyer
            else:
                ship_type = CommonEntityTypes.CG   # Cruiser
                
            # Position in formation (line)
            position = [-30000.0, -2000.0 * i, 0.0]  # West side, staggered north-south
            velocity = [5.0, 0.0, 0.0]  # Moving east at 5 m/s (about 10 knots)
            
            ship_id = self.create_ship(ship_type, ForceID.FRIENDLY, position, velocity)
            blue_ships.append(ship_id)
        
        # Create red force ships
        red_ships = []
        for i in range(num_red_ships):
            # Russian ships (east side)
            if i % 2 == 0:
                ship_type = CommonEntityTypes.KIROV  # Cruiser
            else:
                ship_type = CommonEntityTypes.SLAVA  # Cruiser
                
            # Position in formation (line)
            position = [30000.0, -2000.0 * i, 0.0]  # East side, staggered north-south
            velocity = [-5.0, 0.0, 0.0]  # Moving west at 5 m/s (about 10 knots)
            
            ship_id = self.create_ship(ship_type, ForceID.OPPOSING, position, velocity)
            red_ships.append(ship_id)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            self.update_simulation()
            
            # Check if all ships on one side are destroyed
            blue_alive = [s for s in blue_ships if s in self.entities and self.entities[s]['alive']]
            red_alive = [s for s in red_ships if s in self.entities and self.entities[s]['alive']]
            
            if not blue_alive or not red_alive:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Naval Battle scenario completed at time {self.current_time:.1f} seconds")
        print(f"Blue ships alive: {len(blue_alive)}/{num_blue_ships}")
        print(f"Red ships alive: {len(red_alive)}/{num_red_ships}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def generate_coastal_raid_scenario(self, duration=240.0, num_blue_ships=1, num_red_vehicles=4):
        """Generate a coastal raid scenario with ships engaging land targets
        
        Args:
            duration: Duration of the scenario in seconds
            num_blue_ships: Number of blue force ships
            num_red_vehicles: Number of red force land vehicles
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create blue force ships
        blue_ships = []
        for i in range(num_blue_ships):
            ship_type = CommonEntityTypes.DDG  # Destroyer
                
            # Position offshore
            position = [-10000.0 + (i * 3000.0), -5000.0, 0.0]  # Offshore, slightly staggered
            velocity = [0.0, 5.0, 0.0]  # Moving north parallel to shore
            
            ship_id = self.create_ship(ship_type, ForceID.FRIENDLY, position, velocity)
            blue_ships.append(ship_id)
        
        # Create red force land vehicles
        red_vehicles = []
        for i in range(num_red_vehicles):
            # Mix of tanks and IFVs
            if i % 2 == 0:
                vehicle_type = CommonEntityTypes.T90   # Tank
            else:
                vehicle_type = CommonEntityTypes.BMP3  # IFV
                
            # Position along coast, scattered
            position = [0.0, 2000.0 * i, random.uniform(5.0, 20.0)]  # Along coastline with varied elevation
            velocity = [0.0, 0.0, 0.0]  # Initially stationary
            
            vehicle_id = self.create_land_vehicle(vehicle_type, ForceID.OPPOSING, position, velocity)
            red_vehicles.append(vehicle_id)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            self.update_simulation()
            
            # At 60 seconds, have land vehicles start moving
            if self.current_time >= 60.0 and self.current_time < 61.0:
                for vehicle_id in red_vehicles:
                    if vehicle_id in self.entities and self.entities[vehicle_id]['alive']:
                        # Set to move away from coast
                        new_velocity = [5.0, random.uniform(-2.0, 2.0), 0.0]  # Moving inland with some variation
                        self.update_entity(vehicle_id, new_velocity=new_velocity)
            
            # Check if all vehicles are destroyed
            red_alive = [v for v in red_vehicles if v in self.entities and self.entities[v]['alive']]
            
            if not red_alive:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Coastal Raid scenario completed at time {self.current_time:.1f} seconds")
        print(f"Blue ships alive: {len([s for s in blue_ships if s in self.entities and self.entities[s]['alive']])}/{num_blue_ships}")
        print(f"Red vehicles alive: {len(red_alive)}/{num_red_vehicles}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def generate_land_battle_scenario(self, duration=240.0, num_blue_vehicles=4, num_red_vehicles=4):
        """Generate a land battle scenario with ground vehicles engaging each other
        
        Args:
            duration: Duration of the scenario in seconds
            num_blue_vehicles: Number of blue force land vehicles
            num_red_vehicles: Number of red force land vehicles
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create blue force land vehicles
        blue_vehicles = []
        for i in range(num_blue_vehicles):
            # Mix of tanks and IFVs
            if i % 2 == 0:
                vehicle_type = CommonEntityTypes.M1A2   # Tank
            else:
                vehicle_type = CommonEntityTypes.M2A3   # IFV
                
            # Position in formation 
            position = [-5000.0, -500.0 * i, 20.0]  # West side in staggered formation
            velocity = [8.0, 0.0, 0.0]  # Moving east at 8 m/s (about 30 km/h)
            
            vehicle_id = self.create_land_vehicle(vehicle_type, ForceID.FRIENDLY, position, velocity)
            blue_vehicles.append(vehicle_id)
        
        # Create red force land vehicles
        red_vehicles = []
        for i in range(num_red_vehicles):
            # Mix of tanks and IFVs
            if i % 2 == 0:
                vehicle_type = CommonEntityTypes.T90   # Tank
            else:
                vehicle_type = CommonEntityTypes.BMP3  # IFV
                
            # Position in formation
            position = [5000.0, -500.0 * i, 20.0]  # East side in staggered formation
            velocity = [-8.0, 0.0, 0.0]  # Moving west at 8 m/s (about 30 km/h)
            
            vehicle_id = self.create_land_vehicle(vehicle_type, ForceID.OPPOSING, position, velocity)
            red_vehicles.append(vehicle_id)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            self.update_simulation()
            
            # Check if all vehicles on one side are destroyed
            blue_alive = [v for v in blue_vehicles if v in self.entities and self.entities[v]['alive']]
            red_alive = [v for v in red_vehicles if v in self.entities and self.entities[v]['alive']]
            
            if not blue_alive or not red_alive:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Land Battle scenario completed at time {self.current_time:.1f} seconds")
        print(f"Blue vehicles alive: {len(blue_alive)}/{num_blue_vehicles}")
        print(f"Red vehicles alive: {len(red_alive)}/{num_red_vehicles}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def save_to_pickle(self, filename="surface_to_surface_pdus.pkl"):
        """Save the PDU data to a pickle file"""
        with open(filename, 'wb') as f:
            pickle.dump(self.pdu_store, f)
        print(f"Saved {len(self.pdu_store)} PDUs to {filename}")
        
    def save_to_parquet(self, filename="surface_to_surface_pdus.parquet"):
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
    generator = SurfaceToSurfacePDUGenerator()
    
    # Generate a naval battle scenario
    print("Generating naval battle scenario...")
    generator.generate_naval_battle_scenario(duration=300.0, num_blue_ships=2, num_red_ships=2)
    
    # Save the generated PDUs
    generator.save_to_pickle("surface_to_surface_naval_battle_pdus.pkl")
    generator.save_to_parquet("surface_to_surface_naval_battle_pdus.parquet")
    
    # Generate a coastal raid scenario
    print("\nGenerating coastal raid scenario...")
    generator = SurfaceToSurfacePDUGenerator()  # Create a new generator
    generator.generate_coastal_raid_scenario(duration=240.0, num_blue_ships=1, num_red_vehicles=4)
    
    # Save the generated PDUs
    generator.save_to_pickle("surface_to_surface_coastal_raid_pdus.pkl")
    generator.save_to_parquet("surface_to_surface_coastal_raid_pdus.parquet")
    
    # Generate a land battle scenario
    print("\nGenerating land battle scenario...")
    generator = SurfaceToSurfacePDUGenerator()  # Create a new generator
    generator.generate_land_battle_scenario(duration=240.0, num_blue_vehicles=4, num_red_vehicles=4)
    
    # Save the generated PDUs
    generator.save_to_pickle("surface_to_surface_land_battle_pdus.pkl")
    generator.save_to_parquet("surface_to_surface_land_battle_pdus.parquet")
    
    print("\nAll surface-to-surface scenarios complete.")

if __name__ == "__main__":
    main()