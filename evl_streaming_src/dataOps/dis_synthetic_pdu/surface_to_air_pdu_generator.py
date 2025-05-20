#!/usr/bin/python

"""
Surface-to-Air Combat DIS PDU Generator

This script generates DIS PDUs for surface-to-air combat scenarios.
It creates Entity State PDUs for SAM sites and aircraft, as well as Fire/Detonation PDUs for missiles.

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

class SurfaceToAirPDUGenerator:
    """Generator class for Surface-to-Air Combat PDUs"""
    
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
    
    def create_sam_site(self, sam_type, force_id, position, orientation=None):
        """Create a Surface-to-Air Missile site entity"""
        entity_id = self.create_entity_id()
        
        # Create an Entity State PDU for the SAM site
        espdu = EntityStatePdu()
        espdu.protocolVersion = 7
        espdu.exerciseID = self.exercise_id
        espdu.entityID = entity_id
        espdu.forceId = force_id
        espdu.entityType = sam_type
        
        # Set position (X, Y, Z in meters)
        espdu.entityLocation = Vector3Double()
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]
        
        # SAM sites typically don't move, so velocity is zero
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = 0.0
        espdu.entityLinearVelocity.y = 0.0
        espdu.entityLinearVelocity.z = 0.0
        
        # Set orientation (Psi, Theta, Phi in radians)
        # Default facing north if not specified
        if orientation is None:
            orientation = [0.0, 0.0, 0.0]  # North-facing
            
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = orientation[0]    # Heading/Yaw
        espdu.entityOrientation.theta = orientation[1]  # Pitch
        espdu.entityOrientation.phi = orientation[2]    # Roll
        
        # Set Dead Reckoning parameters
        espdu.deadReckoningParameters = DeadReckoningParameter()
        espdu.deadReckoningParameters.deadReckoningAlgorithm = 1  # Static (no dead reckoning)
        espdu.deadReckoningParameters.otherParameters = [0] * 15
        
        # Set appearance
        espdu.entityAppearance = 0  # Standard appearance, no special status
        
        # Set marking
        espdu.marking = Marking()
        espdu.marking.setMarkingString(f"SAM{entity_id.entityID:02d}")
        
        # Add to tracking dictionary
        self.entities[entity_id.entityID] = {
            'type': 'sam_site',
            'entity_type': sam_type,
            'entity_id': entity_id,
            'force_id': force_id,
            'position': position.copy(),
            'velocity': [0.0, 0.0, 0.0],
            'orientation': orientation.copy(),
            'alive': True,
            'last_fire_time': 0,
            'detection_range': 75000.0,  # 75 km typical SAM detection range
            'engagement_range': 50000.0,  # 50 km typical SAM engagement range
        }
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(espdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'EntityStatePdu',
            'entity_id': entity_id.entityID,
            'force_id': force_id,
            'entity_kind': sam_type.entityKind,
            'entity_domain': sam_type.entityDomain,
            'entity_country': sam_type.entityCountry,
            'entity_category': sam_type.entityCategory,
            'entity_subcategory': sam_type.entitySubcategory,
            'position': position.copy(),
            'velocity': [0.0, 0.0, 0.0],
            'orientation': orientation.copy(),
            'scenario': 'surface-to-air',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def create_aircraft(self, aircraft_type, force_id, position, velocity, orientation=None):
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
        espdu.entityLocation.x = position[0]
        espdu.entityLocation.y = position[1]
        espdu.entityLocation.z = position[2]
        
        # Set velocity (X, Y, Z in meters/sec)
        espdu.entityLinearVelocity = Vector3Float()
        espdu.entityLinearVelocity.x = velocity[0]
        espdu.entityLinearVelocity.y = velocity[1]
        espdu.entityLinearVelocity.z = velocity[2]
        
        # Set orientation (Psi, Theta, Phi in radians)
        if orientation is None:
            # Calculate orientation from velocity vector
            heading = math.atan2(velocity[1], velocity[0])
            pitch = math.asin(velocity[2] / math.sqrt(velocity[0]**2 + velocity[1]**2 + velocity[2]**2)) if any(velocity) else 0
            roll = 0.0
            orientation = [heading, pitch, roll]
        
        espdu.entityOrientation = Orientation()
        espdu.entityOrientation.psi = orientation[0]    # Heading/Yaw
        espdu.entityOrientation.theta = orientation[1]  # Pitch
        espdu.entityOrientation.phi = orientation[2]    # Roll
        
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
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
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
            'position': position.copy(),
            'velocity': velocity.copy(),
            'orientation': orientation.copy(),
            'scenario': 'surface-to-air',
            'pdu_binary': pdu_data
        })
        
        return entity_id.entityID
    
    def update_aircraft(self, entity_id, new_position, new_velocity, new_orientation=None):
        """Update an aircraft's state"""
        if entity_id not in self.entities or not self.entities[entity_id]['alive']:
            return None
        
        entity = self.entities[entity_id]
        if entity['type'] != 'aircraft':
            return None
        
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
            'scenario': 'surface-to-air',
            'pdu_binary': pdu_data
        })
        
        return entity_id
    
    def fire_sam_missile(self, sam_site_id, target_id, missile_type=None):
        """Fire a surface-to-air missile from a SAM site at a target"""
        if sam_site_id not in self.entities or not self.entities[sam_site_id]['alive']:
            return None
            
        if target_id not in self.entities or not self.entities[target_id]['alive']:
            return None
            
        sam_site = self.entities[sam_site_id]
        target = self.entities[target_id]
        
        if sam_site['type'] != 'sam_site' or target['type'] != 'aircraft':
            return None
            
        # Get the appropriate missile type for the SAM site if not specified
        if missile_type is None:
            missile_type = CommonEntityTypes.get_sam_missile_for_site(sam_site['entity_type'])
        
        # Create missile entity
        missile_entity_id = self.create_entity_id()
        event_id = self.create_event_id()
        
        # Create Fire PDU
        fire_pdu = FirePdu()
        fire_pdu.protocolVersion = 7
        fire_pdu.exerciseID = self.exercise_id
        
        # Set firing entity (SAM site)
        fire_pdu.firingEntityID = sam_site['entity_id']
        
        # Set target entity
        fire_pdu.targetEntityID = target['entity_id']
        
        # Set event ID
        fire_pdu.eventID = event_id
        
        # Set munition ID
        fire_pdu.munitionExpendableID = missile_entity_id
        
        # Set fire mission index
        fire_pdu.fireMissionIndex = 0
        
        # Set location (same as SAM site's position)
        fire_pdu.location = Vector3Double()
        fire_pdu.location.x = sam_site['position'][0]
        fire_pdu.location.y = sam_site['position'][1]
        fire_pdu.location.z = sam_site['position'][2]
        
        # Set descriptor
        fire_pdu.descriptor = MunitionDescriptor()
        fire_pdu.descriptor.munitionType = missile_type
        fire_pdu.descriptor.warhead = 1000  # Conventional warhead
        fire_pdu.descriptor.fuse = 1000     # Proximity fuse
        fire_pdu.descriptor.quantity = 1
        fire_pdu.descriptor.rate = 0
        
        # Calculate initial missile direction towards target
        dx = target['position'][0] - sam_site['position'][0]
        dy = target['position'][1] - sam_site['position'][1]
        dz = target['position'][2] - sam_site['position'][2]
        distance = math.sqrt(dx*dx + dy*dy + dz*dz)
        
        # Normalize direction
        if distance > 0:
            dx /= distance
            dy /= distance
            dz /= distance
        
        # Set initial missile velocity (with lead for target's movement)
        # SAM missiles typically have high acceleration but start at moderate speed
        initial_speed = 300.0  # m/s initial speed
        
        # Lead the target by estimating where it will be
        lead_time = distance / 800.0  # Assume average missile speed of 800 m/s
        
        # Estimated target position
        target_future_pos = [
            target['position'][0] + target['velocity'][0] * lead_time,
            target['position'][1] + target['velocity'][1] * lead_time,
            target['position'][2] + target['velocity'][2] * lead_time
        ]
        
        # Direction to future position
        dx_lead = target_future_pos[0] - sam_site['position'][0]
        dy_lead = target_future_pos[1] - sam_site['position'][1]
        dz_lead = target_future_pos[2] - sam_site['position'][2]
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
            'force_id': sam_site['force_id'],
            'position': sam_site['position'].copy(),
            'velocity': missile_velocity.copy(),
            'orientation': [math.atan2(dy_final, dx_final), math.asin(dz_final), 0.0],
            'shooter_id': sam_site_id,
            'target_id': target_id,
            'event_id': event_id,
            'alive': True,
            'flight_time': 0.0,
            'max_speed': 1000.0,  # m/s, typical SAM missile max speed
            'acceleration': 100.0  # m/s^2, typical SAM missile acceleration
        }
        
        # Update the SAM site's last fire time
        sam_site['last_fire_time'] = self.current_time
        
        # Serialize and add to PDU store
        pdu_data = self.serialize_pdu(fire_pdu)
        self.pdu_store.append({
            'timestamp': self.current_time,
            'pdu_type': 'FirePdu',
            'entity_id': missile_entity_id.entityID,
            'force_id': sam_site['force_id'],
            'shooter_id': sam_site_id,
            'target_id': target_id,
            'entity_kind': missile_type.entityKind,
            'entity_domain': missile_type.entityDomain,
            'entity_country': missile_type.entityCountry,
            'entity_category': missile_type.entityCategory,
            'entity_subcategory': missile_type.entitySubcategory,
            'position': sam_site['position'].copy(),
            'velocity': missile_velocity.copy(),
            'scenario': 'surface-to-air',
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
            'scenario': 'surface-to-air',
            'pdu_binary': pdu_data
        })
        
        return missile_id
    
    def detonate_missile(self, missile_id, detonation_result=DetonationResult.ENTITY_PROXIMATE_DETONATION):
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
        
        # If detonation was a hit on target, mark target as destroyed
        if detonation_result == DetonationResult.ENTITY_IMPACT and target_id and target_id in self.entities:
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
            'scenario': 'surface-to-air',
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
            
            # If entity is a missile, update its flight time and velocity
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
                    if distance < 30.0:  # Proximity detonation range (30m)
                        # Determine detonation result (hit or miss)
                        # For SAM missiles, higher probability of hit depending on country
                        hit_probability = 0.7  # 70% base hit probability
                        if entity['entity_type'].entityCountry == CountryCodes.USA:
                            hit_probability = 0.8  # 80% for US systems (slightly better)
                            
                        result = DetonationResult.ENTITY_IMPACT if random.random() < hit_probability else DetonationResult.ENTITY_PROXIMATE_DETONATION
                        self.detonate_missile(entity_id, result)
                        continue
                    
                    # Adjust missile velocity to track target (more aggressive guidance)
                    if distance > 0.1:  # Avoid division by zero
                        # Lead the target based on its velocity
                        lead_time = distance / 800.0  # Assume average closing speed
                        
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
                        max_turn_rate = min(0.9, 0.3 + entity['flight_time'] * 0.1)  # Increases over time
                        
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
                if entity['flight_time'] > 80.0:  # 80 seconds max flight time for SAM
                    self.detonate_missile(entity_id, DetonationResult.NONE_DUD)
                    continue
                
                # Generate Entity State PDU for the missile
                self.update_missile(entity_id)
                
            # For aircraft, generate an Entity State PDU periodically
            elif entity['type'] == 'aircraft' and self.current_time % 5.0 < self.time_step:
                self.update_aircraft(entity_id, entity['position'], entity['velocity'], entity['orientation'])
                
            # For SAM sites, look for targets to engage
            elif entity['type'] == 'sam_site':
                # Check if the SAM site is ready to fire (cooldown period)
                if self.current_time - entity['last_fire_time'] < 8.0:  # 8 seconds between launches
                    continue
                    
                # Look for aircraft in range
                for target_id, target in self.entities.items():
                    if not target['alive'] or target['type'] != 'aircraft' or target['force_id'] == entity['force_id']:
                        continue
                        
                    # Calculate distance to target
                    dx = target['position'][0] - entity['position'][0]
                    dy = target['position'][1] - entity['position'][1]
                    dz = target['position'][2] - entity['position'][2]
                    distance = math.sqrt(dx*dx + dy*dy + dz*dz)
                    
                    # Check if target is within engagement range
                    if distance <= entity['engagement_range']:
                        # 80% chance to fire at target in range (some randomness)
                        if random.random() < 0.8:
                            self.fire_sam_missile(entity_id, target_id)
                            break  # Only fire at one target at a time
    
    def generate_single_sam_vs_aircraft_scenario(self, duration=120.0):
        """Generate a simple scenario with one SAM site engaging one aircraft
        
        Args:
            duration: Duration of the scenario in seconds
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create SAM site
        sam_type = CommonEntityTypes.get_random_sam_site(CountryCodes.RUSSIA)
        sam_position = [0.0, 0.0, 0.0]  # Origin position
        sam_id = self.create_sam_site(sam_type, ForceID.OPPOSING, sam_position)
        
        # Create aircraft (approaching the SAM site)
        aircraft_type = AircraftType.get_random_us_aircraft()
        aircraft_position = [60000.0, 10000.0, 5000.0]  # 60km east, 10km north, 5km altitude
        aircraft_velocity = [-250.0, 0.0, 0.0]  # Moving west at 250 m/s
        aircraft_id = self.create_aircraft(aircraft_type, ForceID.FRIENDLY, aircraft_position, aircraft_velocity)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            self.update_simulation()
            
            # Check if aircraft has been destroyed
            if aircraft_id not in self.entities or not self.entities[aircraft_id]['alive']:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Single SAM vs Aircraft scenario completed at time {self.current_time:.1f} seconds")
        print(f"Aircraft survived: {aircraft_id in self.entities and self.entities[aircraft_id]['alive']}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def generate_air_defense_network_scenario(self, duration=180.0, num_sam_sites=3, num_aircraft=4):
        """Generate a complex scenario with multiple SAM sites engaging multiple aircraft
        
        Args:
            duration: Duration of the scenario in seconds
            num_sam_sites: Number of SAM sites
            num_aircraft: Number of aircraft
        """
        # Initialize scenario time
        self.current_time = 0.0
        self.entity_counter = 1
        self.event_counter = 1
        self.pdu_store = []
        self.entities = {}
        
        # Create SAM sites in a defensive line
        sam_sites = []
        for i in range(num_sam_sites):
            # Position SAMs along a defensive line with some offset
            sam_type = CommonEntityTypes.get_random_sam_site(CountryCodes.RUSSIA)
            x_pos = 0.0
            y_pos = -20000.0 + (i * 20000.0)  # Spread along north-south line
            z_pos = 0.0
            sam_position = [x_pos, y_pos, z_pos]
            
            # Calculate orientation to face east (from where aircraft will approach)
            orientation = [0.0, 0.0, 0.0]  # East-facing
            
            sam_id = self.create_sam_site(sam_type, ForceID.OPPOSING, sam_position, orientation)
            sam_sites.append(sam_id)
        
        # Create aircraft approaching from different angles
        aircraft = []
        for i in range(num_aircraft):
            # Position aircraft in different approach vectors
            aircraft_type = AircraftType.get_random_us_aircraft()
            
            # Angle of approach (spread across eastern hemisphere)
            angle = math.pi/2 - (i * math.pi) / (num_aircraft - 1)  # Spread from northeast to southeast
            distance = 80000.0  # 80km away
            
            x_pos = distance * math.cos(angle)
            y_pos = distance * math.sin(angle)
            z_pos = 3000.0 + random.uniform(0, 4000)  # Altitude between 3km and 7km
            
            aircraft_position = [x_pos, y_pos, z_pos]
            
            # Calculate velocity vector (flying towards center)
            speed = 200.0 + random.uniform(0, 100)  # Speed between 200-300 m/s
            vx = -speed * math.cos(angle)
            vy = -speed * math.sin(angle)
            vz = 0.0
            
            aircraft_velocity = [vx, vy, vz]
            
            aircraft_id = self.create_aircraft(aircraft_type, ForceID.FRIENDLY, aircraft_position, aircraft_velocity)
            aircraft.append(aircraft_id)
        
        # Run the simulation for the specified duration
        while self.current_time < duration:
            self.update_simulation()
            
            # Check if all aircraft have been destroyed
            aircraft_alive = [a for a in aircraft if a in self.entities and self.entities[a]['alive']]
            if not aircraft_alive:
                # Run for a bit longer to show the aftermath
                for _ in range(10):
                    self.update_simulation()
                break
        
        print(f"Air Defense Network scenario completed at time {self.current_time:.1f} seconds")
        print(f"SAM sites alive: {len([s for s in sam_sites if s in self.entities and self.entities[s]['alive']])}/{num_sam_sites}")
        print(f"Aircraft alive: {len([a for a in aircraft if a in self.entities and self.entities[a]['alive']])}/{num_aircraft}")
        print(f"Total PDUs generated: {len(self.pdu_store)}")
        
        return self.pdu_store
    
    def save_to_pickle(self, filename="surface_to_air_pdus.pkl"):
        """Save the PDU data to a pickle file"""
        with open(filename, 'wb') as f:
            pickle.dump(self.pdu_store, f)
        print(f"Saved {len(self.pdu_store)} PDUs to {filename}")
        
    def save_to_parquet(self, filename="surface_to_air_pdus.parquet"):
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
    generator = SurfaceToAirPDUGenerator()
    
    # Generate a simple SAM vs Aircraft scenario
    print("Generating single SAM vs aircraft scenario...")
    generator.generate_single_sam_vs_aircraft_scenario(duration=120.0)
    
    # Save the generated PDUs
    generator.save_to_pickle("surface_to_air_single_sam_pdus.pkl")
    generator.save_to_parquet("surface_to_air_single_sam_pdus.parquet")
    
    # Generate a complex air defense network scenario
    print("\nGenerating air defense network scenario...")
    generator = SurfaceToAirPDUGenerator()  # Create a new generator
    generator.generate_air_defense_network_scenario(duration=180.0, num_sam_sites=3, num_aircraft=4)
    
    # Save the generated PDUs
    generator.save_to_pickle("surface_to_air_network_pdus.pkl")
    generator.save_to_parquet("surface_to_air_network_pdus.parquet")
    
    print("\nAll surface-to-air scenarios complete.")

if __name__ == "__main__":
    main()