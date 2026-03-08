#!/usr/bin/env python3
"""
Restroom sensor simulator with real-time MongoDB change detection and custom DB support
- Connects to MongoDB to fetch ownerId, buildingId, restroomId from sensors collection
- Creates per-sensor MySQL tables (CREATE TABLE IF NOT EXISTS)
- Every 30 seconds generates realistic dummy values for each sensor type
- Uses the actual sensor document data (restroomId, buildingId, sensorId) from MongoDB
- Only processes sensors where isConnected=true
- Automatically detects changes in MongoDB sensors collection and reloads data
- Supports custom database routing per owner (checks auths collection)
- **NEW: Checks alerts collection and creates notifications when thresholds are breached**
- Uses pymongo and pymysql
"""

import time
import random
import datetime
import traceback
import threading
from typing import Dict, Any, List, Optional
import pytz
from pymongo import MongoClient
from bson import ObjectId
import pymysql

# ---------------------------
# Configuration (edit if needed)
# ---------------------------

# MongoDB (as provided)
MONGO_URI = "mongodb+srv://hamzajani:hamzamongo.55@cluster0.n92vrzt.mongodb.net/rest-room"
MONGO_DB_NAME = "rest-room"
MONGO_RESTROOM_COLLECTION = "restrooms"
MONGO_SENSOR_COLLECTION = "sensors"
MONGO_AUTH_COLLECTION = "auths"
MONGO_ALERTS_COLLECTION = "alerts"  # NEW
MONGO_NOTIFICATIONS_COLLECTION = "notifications"  # NEW
MONGO_RULES_COLLECTION_CANDIDATES = ["rules", "restroomRules", "rest-room rules"]

# MySQL (as provided) - Default DB
SQL_DB_NAME = "final-test-restrrom"
SQL_HOST_NAME = "5.223.56.233"
SQL_USERNAME = "admin"
SQL_PASSWORD = "security890"
SQL_PORT = 3306

# Insert interval (30 seconds)
INSERT_INTERVAL_SECONDS = 30

# Flag to signal reload of sensors
sensors_reload_flag = threading.Event()
shutdown_flag = threading.Event()

# Cache for custom DB connections and configurations
custom_db_cache = {}
custom_db_cache_lock = threading.Lock()

# ---------------------------
# MongoDB helpers
# ---------------------------

def connect_mongo(uri: str = MONGO_URI) -> MongoClient:
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.server_info()
    return client

def fetch_owner_db_config(mongo_client: MongoClient, owner_id: str, db_name: str = MONGO_DB_NAME, auth_collection: str = MONGO_AUTH_COLLECTION) -> Optional[Dict[str, Any]]:
    """
    Fetch custom database configuration for a specific owner from auths collection.
    Returns None if owner not found or custom DB not configured.
    """
    try:
        db = mongo_client[db_name]
        owner_obj_id = ObjectId(owner_id)
        auth_doc = db[auth_collection].find_one({"_id": owner_obj_id})
        
        if not auth_doc:
            return None
        
        is_custom_db = auth_doc.get("isCustomDb", False)
        
        if not is_custom_db:
            return None
        
        # Extract custom DB configuration
        custom_config = {
            "host": auth_doc.get("customDbHost"),
            "username": auth_doc.get("customDbUsername"),
            "password": auth_doc.get("customDbPassword"),
            "database": auth_doc.get("customDbName"),
            "port": auth_doc.get("customDbPort", 3306),
            "is_connected": auth_doc.get("isCustomDbConnected", False)
        }
        
        # Validate that all required fields are present
        if all([custom_config["host"], custom_config["username"], 
                custom_config["password"], custom_config["database"]]):
            return custom_config
        else:
            print(f"[WARN] Owner {owner_id} has isCustomDb=true but incomplete configuration")
            return None
            
    except Exception as e:
        print(f"[ERROR] Failed to fetch owner DB config for {owner_id}: {e}")
        return None

def fetch_connected_sensors(mongo_client: MongoClient, 
                           db_name: str = MONGO_DB_NAME, 
                           sensor_collection: str = MONGO_SENSOR_COLLECTION) -> List[Dict[str, Any]]:
    """
    Fetch all connected sensors from MongoDB where isConnected=true.
    Returns a list of sensor info dictionaries.
    """
    db = mongo_client[db_name]
    
    # Fetch all sensors where isConnected=true
    sensor_docs = list(db[sensor_collection].find({"isConnected": True}))
    
    if not sensor_docs:
        print(f"[WARN] No connected sensors found in MongoDB collection '{sensor_collection}' (isConnected=true)")
        return []
    
    # Build sensors list with all required information
    sensors_list = []
    for sensor_doc in sensor_docs:
        sensor_id = str(sensor_doc.get("_id"))
        owner_id = str(sensor_doc.get("ownerId", ""))
        building_id = str(sensor_doc.get("buildingId", ""))
        restroom_id = str(sensor_doc.get("restroomId", ""))
        sensor_type = sensor_doc.get("sensorType", "")
        unique_id = sensor_doc.get("uniqueId", "")
        sensor_name = sensor_doc.get("sensorName") or sensor_doc.get("name") or unique_id or f"sensor-{sensor_id[-6:]}"
        is_connected = sensor_doc.get("isConnected", False)
        
        if is_connected and sensor_type:
            sensors_list.append({
                "sensor_id": sensor_id,
                "owner_id": owner_id,
                "building_id": building_id,
                "restroom_id": restroom_id,
                "sensor_type": sensor_type,
                "sensor_name": sensor_name,
                "unique_id": unique_id,
                "is_connected": is_connected
            })
            print(f"[MongoDB] Loaded sensor: {sensor_name} / {unique_id} (type: {sensor_type}, id: {sensor_id}, owner: {owner_id})")
        else:
            print(f"[WARN] Skipping sensor {sensor_id} - isConnected={is_connected} or missing sensorType")
    
    print(f"[MongoDB] Total connected sensors loaded: {len(sensors_list)}")
    return sensors_list

def get_num_toilets(mongo_client: MongoClient, restroom_id: str, 
                    db_name: str = MONGO_DB_NAME, 
                    restroom_collection: str = MONGO_RESTROOM_COLLECTION) -> int:
    """
    Fetch numOfToilets from restroom document.
    """
    db = mongo_client[db_name]
    rest_doc = db[restroom_collection].find_one({"_id": restroom_id})
    if rest_doc:
        try:
            return int(rest_doc.get("numOfToilets", 4))
        except Exception:
            return 4
    return 4

# ---------------------------
# NEW: Alert and Notification Functions
# ---------------------------

def fetch_active_alerts(mongo_client: MongoClient, 
                       db_name: str = MONGO_DB_NAME, 
                       alerts_collection: str = MONGO_ALERTS_COLLECTION) -> List[Dict[str, Any]]:
    """
    Fetch all active alerts from MongoDB alerts collection
    """
    try:
        db = mongo_client[db_name]
        alerts = list(db[alerts_collection].find({"status": "active"}))
        print(f"[MongoDB] Loaded {len(alerts)} active alerts from {alerts_collection}")
        
        # Debug: Print all alerts with their details
        for alert in alerts:
            print(f"  - {alert.get('name')} (type: {alert.get('alertType')}, owner: {alert.get('ownerId')}, value: {alert.get('value')})")
        
        return alerts
    except Exception as ex:
        print(f"[ERROR] Failed to fetch alerts: {ex}")
        return []


def resolve_rules_collection(db) -> Optional[str]:
    """
    Resolve the rules collection name by checking known candidates.
    """
    try:
        existing = set(db.list_collection_names())
        for name in MONGO_RULES_COLLECTION_CANDIDATES:
            if name in existing:
                return name
    except Exception as ex:
        print(f"[WARN] Failed to resolve rules collection: {ex}")
    return None


def fetch_active_rules(mongo_client: MongoClient, db_name: str = MONGO_DB_NAME) -> List[Dict[str, Any]]:
    """
    Fetch active rule-engine documents from rules collection.
    """
    try:
        db = mongo_client[db_name]
        rules_collection = resolve_rules_collection(db)

        if not rules_collection:
            return []

        rules = list(db[rules_collection].find({"status": "active"}))
        print(f"[MongoDB] Loaded {len(rules)} active rule(s) from {rules_collection}")
        return rules
    except Exception as ex:
        print(f"[ERROR] Failed to fetch rule-engine rules: {ex}")
        return []


def create_notification(mongo_client: MongoClient, 
                       alert_info: Dict[str, Any],
                       sensor_info: Dict[str, Any],
                       triggered_value: Any,
                       message: str,
                       db_name: str = MONGO_DB_NAME,
                       notifications_collection: str = MONGO_NOTIFICATIONS_COLLECTION):
    """
    Create a notification document in MongoDB when an alert is triggered
    """
    try:
        db = mongo_client[db_name]
        
        notification_doc = {
            "alertId": str(alert_info.get('_id')),  # Use _id from alert document
            "alertName": alert_info.get('name'),
            "alertType": alert_info.get('alertType'),
            "severity": alert_info.get('severity'),
            "sensorId": sensor_info.get('sensor_id'),
            "sensorType": sensor_info.get('sensor_type'),
            "sensorName": sensor_info.get('sensor_name'),
            "owner": sensor_info.get('owner_id'),
            "buildingId": sensor_info.get('building_id'),
            "restroomId": sensor_info.get('restroom_id'),
            "triggeredValue": triggered_value,
            "message": message,
            "timestamp": datetime.datetime.now(pytz.UTC),
            "read": False,
            "platform": alert_info.get('platform', 'web'),
            "email": alert_info.get('email')  # Include email if present
        }

        print(f"        📧 Creating notification for alert '{alert_info.get('name')}' with message: {notification_doc}")
        
        result = db[notifications_collection].insert_one(notification_doc)
        print(f"        📧 Notification created: {result.inserted_id}")
        
    except Exception as ex:
        print(f"        [ERROR] Failed to create notification: {ex}")
        traceback.print_exc()


def to_bool_if_possible(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on", "occupied"}:
            return True
        if normalized in {"false", "0", "no", "off", "unoccupied"}:
            return False
    return None


def infer_sensor_value_for_rule(sensor_type: str, data: Dict[str, Any]) -> Any:
    """
    Pick the most relevant current value from generated sensor data for rule comparison.
    """
    if sensor_type == "door_queue":
        return data.get("count")
    if sensor_type == "stall_status":
        return data.get("state")
    if sensor_type == "occupancy":
        return data.get("occupied")
    if sensor_type == "air_quality":
        return data.get("aqi")
    if sensor_type == "toilet_paper":
        return data.get("level")
    if sensor_type == "handwash":
        return data.get("level")
    if sensor_type == "soap_dispenser":
        return data.get("level")
    if sensor_type == "water_leakage":
        return data.get("waterDetected")
    return None


def evaluate_rule_condition(current_value: Any, rule_min: Any, rule_max: Any) -> bool:
    """
    Return True when current_value violates the min/max rule condition.
    """
    if current_value is None:
        return False

    # Boolean / state equality checks when only min is provided
    if rule_max is None and rule_min is not None:
        current_bool = to_bool_if_possible(current_value)
        min_bool = to_bool_if_possible(rule_min)
        if current_bool is not None and min_bool is not None:
            return current_bool == min_bool

        # Fallback to case-insensitive string equality for state-like values
        return str(current_value).strip().lower() == str(rule_min).strip().lower()

    # Numeric threshold checks
    current_num = safe_float(current_value, None)
    min_num = safe_float(rule_min, None)
    max_num = safe_float(rule_max, None)

    if current_num is None:
        return False

    if min_num is not None and current_num < min_num:
        return True
    if max_num is not None and current_num > max_num:
        return True
    return False


def create_ruleengine_notification(mongo_client: MongoClient,
                                   rule_info: Dict[str, Any],
                                   sensor_info: Dict[str, Any],
                                   current_value: Any,
                                   rule_condition: Dict[str, Any],
                                   db_name: str = MONGO_DB_NAME,
                                   notifications_collection: str = MONGO_NOTIFICATIONS_COLLECTION):
    """
    Create a rule-engine notification document in MongoDB notifications collection.
    """
    try:
        db = mongo_client[db_name]

        min_val = rule_condition.get("min")
        max_val = rule_condition.get("max")
        if max_val is None:
            condition_text = f"equals {min_val}"
        else:
            condition_text = f"outside range ({min_val}-{max_val})"

        notification_doc = {
            "type": "ruleengine",
            "ruleId": str(rule_info.get("_id")),
            "ruleName": rule_info.get("name"),
            "severity": rule_info.get("severity"),
            "owner": sensor_info.get("owner_id"),
            "ownerId": sensor_info.get("owner_id"),
            "buildingId": sensor_info.get("building_id"),
            "restroomId": sensor_info.get("restroom_id"),
            "sensorId": sensor_info.get("sensor_id"),
            "sensorType": sensor_info.get("sensor_type"),
            "sensorName": sensor_info.get("sensor_name"),
            "triggeredValue": current_value,
            "ruleCondition": {"min": min_val, "max": max_val},
            "message": f"Rule '{rule_info.get('name', 'Unnamed Rule')}' triggered: value {current_value} {condition_text}",
            "status": "active",
            "timestamp": datetime.datetime.now(pytz.UTC),
            "read": False,
            "platform": rule_info.get("platform", "web"),
            "email": rule_info.get("email")
        }

        result = db[notifications_collection].insert_one(notification_doc)
        print(f"        📣 Rule-engine notification created: {result.inserted_id}")

    except Exception as ex:
        print(f"        [ERROR] Failed to create rule-engine notification: {ex}")
        traceback.print_exc()


def check_rules_for_data(mongo_client: MongoClient,
                         sensor_info: Dict[str, Any],
                         data: Dict[str, Any],
                         active_rules: List[Dict[str, Any]]):
    """
    Evaluate active rules for this sensor and create notifications when triggered.
    """
    if not active_rules:
        return

    sensor_id = sensor_info.get("sensor_id")
    owner_id = sensor_info.get("owner_id")
    building_id = sensor_info.get("building_id")
    restroom_id = sensor_info.get("restroom_id")
    sensor_type = sensor_info.get("sensor_type")

    current_value = infer_sensor_value_for_rule(sensor_type, data)

    for rule in active_rules:
        try:
            rule_sensor_ids = [str(sid) for sid in (rule.get("sensorIds") or [])]
            if sensor_id not in rule_sensor_ids:
                continue

            # Match context to avoid cross-owner/building triggers
            if rule.get("ownerId") and str(rule.get("ownerId")) != str(owner_id):
                continue
            if rule.get("buildingId") and str(rule.get("buildingId")) != str(building_id):
                continue
            if rule.get("restroomId") and str(rule.get("restroomId")) != str(restroom_id):
                continue

            rule_values = (((rule.get("values") or {}).get("value")) or {})
            sensor_rule = rule_values.get(sensor_id) or rule_values.get(str(sensor_id))
            if not isinstance(sensor_rule, dict):
                continue

            rule_min = sensor_rule.get("min")
            rule_max = sensor_rule.get("max")

            if evaluate_rule_condition(current_value, rule_min, rule_max):
                print(f"      ✅ RULE TRIGGERED: {rule.get('name')} for sensor {sensor_id}")
                create_ruleengine_notification(
                    mongo_client,
                    rule,
                    sensor_info,
                    current_value,
                    sensor_rule
                )

        except Exception as ex:
            print(f"      [ERROR] Rule-engine check failed for sensor {sensor_id}: {ex}")
            traceback.print_exc()

def check_alerts_for_data(mongo_client: MongoClient, 
                          sensor_info: Dict[str, Any], 
                          data: Dict[str, Any],
                          alerts_list: List[Dict[str, Any]]):
    """
    Check if any alerts should be triggered based on the sensor data.
    """
    sensor_type = sensor_info['sensor_type']
    owner_id = sensor_info['owner_id']
    sensor_id = sensor_info['sensor_id']
    
    # Map sensor types to alert types
    sensor_to_alert_type = {
        "door_queue": ["doorQueue"],
        "stall_status": ["stallStatus"],
        "occupancy": ["occupancy"],
        "air_quality": ["airQuality"],
        "toilet_paper": ["toiletPaper"],
        "handwash": ["handwash"],
        "soap_dispenser": ["soapDispenser"],
        "water_leakage": ["waterLeakage"]
    }
    
    alert_types_for_sensor = sensor_to_alert_type.get(sensor_type, [])
    if not alert_types_for_sensor:
        return
    
    # Filter alerts for this sensor type and owner
    relevant_alerts = [
        alert for alert in alerts_list
        if alert.get('alertType') in alert_types_for_sensor
        and (alert.get('ownerId') == owner_id or not alert.get('ownerId'))
        and alert.get('status') == 'active'
    ]
    
    if relevant_alerts:
        print(f"    [Alerts] Checking {len(relevant_alerts)} alert(s) for {sensor_type}")
    
    for alert in relevant_alerts:
        alert_type = alert.get('alertType')
        alert_name = alert.get('name', 'Unknown')
        value_config = alert.get('value') or {}
        
        triggered = False
        triggered_value = None
        message = ""
        
        try:
            # Door Queue alerts
            if alert_type == "doorQueue" and sensor_type == "door_queue":
                count = data.get('count')
                if count is not None:
                    # Convert strings to numbers
                    min_count = float(value_config.get('min', 0))
                    max_count = float(value_config.get('max', 999999))

                    print(f"      Queue count: {count}, acceptable range: {min_count}-{max_count}")
                    if count < min_count or count > max_count:
                        triggered = True
                        triggered_value = count
                        message = f"Queue count {count} outside acceptable range ({min_count}-{max_count})"
            
            # Stall Status alerts
            elif alert_type == "stallStatus" and sensor_type == "stall_status":
                state = data.get('state')
                alert_state = value_config.get('min')  # State to monitor
                
                if alert_state and state == alert_state:
                    triggered = True
                    triggered_value = state
                    message = f"Stall status is '{state}'"
                    print(f"      Stall state: {state}, alert triggers on: {alert_state} → TRIGGERED")
            
            # Occupancy alerts
            elif alert_type == "occupancy" and sensor_type == "occupancy":
                occupied = data.get('occupied')
                duration = data.get('occupancyDuration')
                min_val = value_config.get('min')
                max_val = value_config.get('max')

                # Case 1: Boolean trigger — value.min is True/False (trigger when occupied state matches)
                if isinstance(min_val, bool) and occupied is not None:
                    if occupied == min_val:
                        triggered = True
                        triggered_value = occupied
                        message = f"Occupancy state is {'occupied' if occupied else 'unoccupied'}"
                        print(f"      Occupied={occupied}, alert triggers on min={min_val} → TRIGGERED")
                    else:
                        print(f"      Occupied={occupied}, alert triggers on min={min_val} → not triggered")
                # Case 2: Duration threshold — value.max is a number (trigger when duration exceeds max)
                elif duration is not None and max_val is not None:
                    max_duration = safe_float(max_val, 999999)
                    if duration > max_duration:
                        triggered = True
                        triggered_value = duration
                        message = f"Occupancy {duration}s > {max_duration}s"
                else:
                    print(f"      Occupancy alert skipped: unrecognised config (min={min_val}, max={max_val})")
            
            # Air Quality alerts
            elif alert_type == "airQuality" and sensor_type == "air_quality":
                aqi = data.get('aqi')
                if aqi is not None:
                    min_val = float(value_config.get('min', 0))
                    max_val = float(value_config.get('max', 999999))

                    print(f"      AQI: {aqi}, acceptable range: {min_val}-{max_val}")
                    if aqi < min_val or aqi > max_val:
                        triggered = True
                        triggered_value = aqi
                        message = f"AQI {aqi} outside acceptable range ({min_val}-{max_val})"
            
            # Toilet Paper alerts
            elif alert_type == "toiletPaper" and sensor_type == "toilet_paper":
                level = data.get('level')
                if level is not None:
                    min_level = safe_float(value_config.get('min'), None)

                    print(f"Toilet paper level: {level}%, min threshold: {min_level}")

                    # Only trigger if min_level is properly configured
                    if min_level is not None and level < min_level:
                        triggered = True
                        triggered_value = level
                        message = f"Toilet paper low: {level}% < {min_level}%"
                        print(f"Alert condition met: {level} < {min_level}")
                    elif min_level is None:
                        print("Alert skipped: min_level not configured")
            
            # Soap Dispenser alerts
            elif alert_type == "soapDispenser" and sensor_type == "soap_dispenser":
                level = data.get('level')
                if level is not None:
                    min_level = safe_float(value_config.get('min'), None)

                    print(f"Soap level: {level}%, min threshold: {min_level}")

                    # Only trigger if min_level is properly configured
                    if min_level is not None and level < min_level:
                        triggered = True
                        triggered_value = level
                        message = f"Soap low: {level}% < {min_level}%"
                        print(f"Alert condition met: {level} < {min_level}")
                    elif min_level is None:
                        print("Alert skipped: min_level not configured")
            
            # Handwash alerts
            elif alert_type == "handwash" and sensor_type == "handwash":
                level = data.get('level')
                if level is not None:
                    min_level = safe_float(value_config.get('min'), None)

                    print(f"      Handwash level: {level}%, min threshold: {min_level}")

                    # Only trigger if min_level is properly configured
                    if min_level is not None and level < min_level:
                        triggered = True
                        triggered_value = level
                        message = f"Handwash low: {level}% < {min_level}%"
                        print(f"      Alert condition met: {level} < {min_level}")
                    elif min_level is None:
                        print(f"Alert skipped: min_level not configured")
            
            # Water Leakage alerts
            elif alert_type == "waterLeakage" and sensor_type == "water_leakage":
                if data.get('waterDetected'):
                    triggered = True
                    triggered_value = data.get('waterLevel_mm', 0)
                    message = f"Water leakage detected: {triggered_value}mm"
            
            # Create notification if triggered
            if triggered:
                print(f"      ✅ TRIGGERED: {alert_name}")
                create_notification(mongo_client, alert, sensor_info, triggered_value, message)
        
        except Exception as ex:
            print(f"      [ERROR] Alert check failed: {ex}")
            traceback.print_exc()

# ---------------------------
# END NEW ALERT FUNCTIONS
# ---------------------------

def monitor_sensor_changes(mongo_client: MongoClient, db_name: str = MONGO_DB_NAME):
    """
    Monitor entire MongoDB database for changes using change streams.
    Sets the reload flag when changes are detected in any collection.
    """
    print("[ChangeStream] Starting MongoDB change stream monitor...")
    try:
        db = mongo_client[db_name]
        # Watch for insert, update, delete, and replace operations on any collection
        with db.watch([
            {'$match': {
                'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}
            }}
        ]) as stream:
            print("[ChangeStream] Monitoring entire database for changes...")
            for change in stream:
                if shutdown_flag.is_set():
                    break
                
                operation = change.get('operationType', 'unknown')
                doc_id = change.get('documentKey', {}).get('_id', 'unknown')
                collection = change.get('ns', {}).get('coll', 'unknown')
                
                print(f"\n[ChangeStream] Detected {operation} operation in collection '{collection}' on document {doc_id}")
                print("[ChangeStream] Triggering sensor reload...")
                
                # Clear custom DB cache when auths collection changes
                if collection == MONGO_AUTH_COLLECTION:
                    with custom_db_cache_lock:
                        custom_db_cache.clear()
                        print("[ChangeStream] Cleared custom DB cache due to auths collection change")
                
                # Set the reload flag
                sensors_reload_flag.set()
                
    except Exception as e:
        if not shutdown_flag.is_set():
            print(f"[ChangeStream ERROR] {e}")
            print("[ChangeStream] Will retry monitoring in 10 seconds...")
            time.sleep(10)
            if not shutdown_flag.is_set():
                monitor_sensor_changes(mongo_client, db_name)

# ---------------------------
# MySQL helpers
# ---------------------------

def connect_mysql(host=SQL_HOST_NAME, user=SQL_USERNAME, password=SQL_PASSWORD, 
                 db=SQL_DB_NAME, port=SQL_PORT):
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db,
        port=port,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    return conn

def get_db_connection(owner_id: str, mongo_client: MongoClient) -> pymysql.connections.Connection:
    """
    Get appropriate database connection for the owner.
    Returns custom DB connection if configured, otherwise default DB connection.
    """
    # Check cache first
    with custom_db_cache_lock:
        if owner_id in custom_db_cache:
            cached = custom_db_cache[owner_id]
            if cached["use_custom"]:
                # Try to use cached connection
                try:
                    cached["connection"].ping(reconnect=True)
                    return cached["connection"]
                except Exception:
                    # Connection dead, remove from cache
                    del custom_db_cache[owner_id]
            else:
                # Use default connection
                return cached["connection"]
    
    # Fetch owner's DB configuration
    custom_config = fetch_owner_db_config(mongo_client, owner_id)
    
    if custom_config:
        # Try to connect to custom database
        try:
            print(f"[CustomDB] Connecting to custom DB for owner {owner_id}: {custom_config['host']}:{custom_config['port']}/{custom_config['database']}")
            custom_conn = connect_mysql(
                host=custom_config["host"],
                user=custom_config["username"],
                password=custom_config["password"],
                db=custom_config["database"],
                port=custom_config["port"]
            )
            
            # Create tables in custom DB
            create_tables(custom_conn)
            
            # Cache the connection
            with custom_db_cache_lock:
                custom_db_cache[owner_id] = {
                    "use_custom": True,
                    "connection": custom_conn,
                    "config": custom_config
                }
            
            print(f"[CustomDB] Successfully connected to custom DB for owner {owner_id}")
            return custom_conn
            
        except Exception as e:
            print(f"[CustomDB ERROR] Failed to connect to custom DB for owner {owner_id}: {e}")
            print(f"[CustomDB] Falling back to default DB for owner {owner_id}")
    
    # Use default connection
    default_conn = connect_mysql()
    
    # Cache the default connection decision
    with custom_db_cache_lock:
        custom_db_cache[owner_id] = {
            "use_custom": False,
            "connection": default_conn,
            "config": None
        }
    
    return default_conn

def create_tables(conn):
    """
    Create all sensor tables (CREATE TABLE IF NOT EXISTS)
    """
    with conn.cursor() as cur:
        # Door Queue Sensor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS door_queue (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                event VARCHAR(16),
                `count` INT,
                queueState VARCHAR(32),
                windowCount INT
            ) ENGINE=InnoDB;
        """)
        
        # Stall / Door Status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stall_status (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                `state` VARCHAR(32),
                usageCount INT
            ) ENGINE=InnoDB;
        """)
        
        # Occupancy Sensor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS occupancy (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                occupied BOOLEAN,
                occupancyDuration INT,
                lastOccupiedAt DATETIME
            ) ENGINE=InnoDB;
        """)
        
        # Air Quality Sensor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS air_quality (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                tvoc FLOAT,
                eCO2 INT,
                pm2_5 FLOAT,
                aqi INT,
                smellLevel VARCHAR(32)
            ) ENGINE=InnoDB;
        """)
        
        # Toilet Paper Level
        cur.execute("""
            CREATE TABLE IF NOT EXISTS toilet_paper (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                `level` INT,
                `status` VARCHAR(32),
                lastRefilledAt DATETIME
            ) ENGINE=InnoDB;
        """)
        
        # Handwash Sensor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS handwash (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                dispenseEvent VARCHAR(32),
                lastDispenseAt DATETIME,
                `level` INT,
                `status` VARCHAR(32)
            ) ENGINE=InnoDB;
        """)
        
        # Soap Dispenser
        cur.execute("""
            CREATE TABLE IF NOT EXISTS soap_dispenser (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                dispenseEvent VARCHAR(32),
                lastDispenseAt DATETIME,
                `level` INT,
                `status` VARCHAR(32)
            ) ENGINE=InnoDB;
        """)
        
        # Water Leakage
        cur.execute("""
            CREATE TABLE IF NOT EXISTS water_leakage (
                idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
                ownerId VARCHAR(128),
                buildingId VARCHAR(128),
                restroomId VARCHAR(128),
                stallId VARCHAR(64),
                sensorId VARCHAR(128),
                sensor_unique_id VARCHAR(128),
                `timestamp` DATETIME,
                waterDetected BOOLEAN,
                waterLevel_mm FLOAT
            ) ENGINE=InnoDB;
        """)
    
    conn.commit()
    print("[MySQL] Created tables (if not existed)")

# ---------------------------
# Helper functions
# ---------------------------

def safe_float(value, default: float = 0.0) -> float:
    """
    Safely convert a value to float with a default fallback.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def now_dt():
    # Return current time in UTC (converted from Jeddah time)
    tz = pytz.timezone('Asia/Riyadh')
    dt_local = datetime.datetime.now(tz)
    return dt_local.astimezone(pytz.UTC).replace(tzinfo=None)

def choose_stall(num_toilets: int) -> str:
    if not num_toilets or num_toilets <= 0:
        return "stall-1"
    return f"stall-{random.randint(1, max(1, num_toilets))}"

# ---------------------------
# Dummy value generators (one per sensor type)
# ---------------------------

def generate_dummy_door_queue(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    event = random.choices(["enter", "exit", "none"], weights=[25, 25, 50])[0]
    # Generate higher queue counts to trigger alerts
    count = random.randint(15, 50) if event != "none" else random.randint(5, 20)
    
    if count == 0:
        queue_state = "idle"
    elif count < 10:
        queue_state = "forming"
    else:
        queue_state = "full"
    
    window_count = max(0, count + random.randint(-3, 5))
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        event=event,
        count=count,
        queueState=queue_state,
        windowCount=window_count
    )

def generate_dummy_stall_status(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    state = random.choice(["open", "occupied", "locked", "vacant"])
    usageCount = random.randint(50, 500)
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        state=state,
        usageCount=usageCount
    )

def generate_dummy_occupancy(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    occupied = random.choice([True, False])
    # Generate longer occupancy durations to trigger alerts
    occupancyDuration = random.randint(1200, 3600) if occupied else 0
    
    if not occupied:
        lastOccupiedAt = ts - datetime.timedelta(seconds=random.randint(60, 7200))
    else:
        lastOccupiedAt = ts - datetime.timedelta(seconds=random.randint(1, occupancyDuration))
    
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        occupied=occupied,
        occupancyDuration=occupancyDuration,
        lastOccupiedAt=lastOccupiedAt
    )

def generate_dummy_air_quality(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    # Generate higher TVOC and eCO2 values to trigger alerts
    tvoc = round(random.uniform(1500.0, 3000.0), 2)
    eCO2 = random.randint(2000, 4000)
    pm2_5 = round(random.uniform(50.0, 200.0), 2)
    
    # Generate higher AQI values to trigger alerts
    if pm2_5 <= 12:
        aqi = random.randint(100, 150)
    elif pm2_5 <= 35.4:
        aqi = random.randint(120, 180)
    elif pm2_5 <= 55.4:
        aqi = random.randint(140, 200)
    else:
        aqi = random.randint(200, 350)
    
    smellLevel = random.choice(["none", "slight", "moderate", "strong"])
    stallId = "common"
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        tvoc=tvoc,
        eCO2=eCO2,
        pm2_5=pm2_5,
        aqi=aqi,
        smellLevel=smellLevel
    )

def generate_dummy_toilet_paper(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    # Generate realistic levels - sometimes high, sometimes low
    level = random.randint(10, 95)  # More realistic: 10-95%
    status = "ok" if level > 30 else "low"
    lastRefilledAt = ts - datetime.timedelta(hours=random.randint(24, 72))
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        level=level,
        status=status,
        lastRefilledAt=lastRefilledAt
    )

def generate_dummy_handwash(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    dispenseEvent = random.choice(["dispense", "none", "dispense", "dispense"])

    if dispenseEvent == "dispense":
        lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(30, 1800))
    else:
        lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(3600, 14400))

    # Generate realistic levels - sometimes high, sometimes low
    level = random.randint(10, 95)  # More realistic: 10-95%
    status = "ok" if level > 25 else "refill_required"
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        dispenseEvent=dispenseEvent,
        lastDispenseAt=lastDispenseAt,
        level=level,
        status=status
    )

def generate_dummy_soap_dispenser(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    dispenseEvent = random.choice(["dispense", "none", "dispense", "dispense"])

    if dispenseEvent == "dispense":
        lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(30, 1800))
    else:
        lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(3600, 14400))

    # Generate realistic levels - sometimes high, sometimes low
    level = random.randint(10, 95)  # More realistic: 10-95%
    status = "ok" if level > 30 else "refill_required"
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        dispenseEvent=dispenseEvent,
        lastDispenseAt=lastDispenseAt,
        level=level,
        status=status
    )

def generate_dummy_water_leakage(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
    ts = now_dt()
    # Increase probability of water detection to trigger alerts
    waterDetected = random.choices([False, True], weights=[30, 70])[0]
    waterLevel_mm = round(random.uniform(50.0, 300.0), 1)
    stallId = choose_stall(num_toilets)
    
    return dict(
        ownerId=sensor_info['owner_id'],
        buildingId=sensor_info['building_id'],
        restroomId=sensor_info['restroom_id'],
        stallId=stallId,
        sensorId=sensor_info['sensor_id'],
        sensor_unique_id=sensor_info['unique_id'],
        timestamp=ts,
        waterDetected=waterDetected,
        waterLevel_mm=waterLevel_mm
    )

# ---------------------------
# MySQL insert functions
# ---------------------------

def insert_door_queue(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO door_queue 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, event, `count`, queueState, windowCount)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['event'],
              data['count'], data['queueState'], data['windowCount']))
    conn.commit()

def insert_stall_status(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stall_status 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, `state`, usageCount)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['state'],
              data['usageCount']))
    conn.commit()

def insert_occupancy(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO occupancy 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, occupied, occupancyDuration, lastOccupiedAt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
              int(data['occupied']), data['occupancyDuration'], data['lastOccupiedAt']))
    conn.commit()

def insert_air_quality(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO air_quality 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, tvoc, eCO2, pm2_5, aqi, smellLevel)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['tvoc'],
              data['eCO2'], data['pm2_5'], data['aqi'], data['smellLevel']))
    conn.commit()

def insert_toilet_paper(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO toilet_paper 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, `level`, `status`, lastRefilledAt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['level'],
              data['status'], data['lastRefilledAt']))
    conn.commit()

def insert_handwash(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO handwash 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, dispenseEvent, lastDispenseAt, `level`, `status`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
              data['dispenseEvent'], data['lastDispenseAt'], data['level'], data['status']))
    conn.commit()

def insert_soap_dispenser(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO soap_dispenser 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, dispenseEvent, lastDispenseAt, `level`, `status`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
              data['dispenseEvent'], data['lastDispenseAt'], data['level'], data['status']))
    conn.commit()

def insert_water_leakage(conn, data: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO water_leakage 
            (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
             `timestamp`, waterDetected, waterLevel_mm)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
              data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
              int(data['waterDetected']), data['waterLevel_mm']))
    conn.commit()

# ---------------------------
# Sensor type mapping
# ---------------------------

SENSOR_TYPE_GENERATORS = {
    "door_queue": generate_dummy_door_queue,
    "stall_status": generate_dummy_stall_status,
    "occupancy": generate_dummy_occupancy,
    "air_quality": generate_dummy_air_quality,
    "toilet_paper": generate_dummy_toilet_paper,
    "handwash": generate_dummy_handwash,
    "soap_dispenser": generate_dummy_soap_dispenser,
    "water_leakage": generate_dummy_water_leakage,
}

SENSOR_TYPE_INSERTERS = {
    "door_queue": insert_door_queue,
    "stall_status": insert_stall_status,
    "occupancy": insert_occupancy,
    "air_quality": insert_air_quality,
    "toilet_paper": insert_toilet_paper,
    "handwash": insert_handwash,
    "soap_dispenser": insert_soap_dispenser,
    "water_leakage": insert_water_leakage,
}

# ---------------------------
# Main loop
# ---------------------------

def main_loop():
    mongo_client = None
    default_mysql_conn = None
    change_stream_thread = None
    
    try:
        # Connect to MongoDB
        print("[MongoDB] Connecting...")
        mongo_client = connect_mongo()
        
        # Connect to default MySQL and create tables once
        print("[MySQL] Connecting to default database...")
        default_mysql_conn = connect_mysql()
        create_tables(default_mysql_conn)
        
        # Start change stream monitor in separate thread
        change_stream_thread = threading.Thread(
            target=monitor_sensor_changes,
            args=(mongo_client,),
            daemon=True
        )
        change_stream_thread.start()
        
        # Initial sensor load
        sensors_list = fetch_connected_sensors(mongo_client)
        print(f"\n[Simulator] Starting main loop - processing {len(sensors_list)} connected sensors")
        print("[Simulator] Custom DB routing enabled - checking auths collection per owner")
        print("[Simulator] Press Ctrl+C to stop\n")
        
        while True:
            # Check if reload is needed
            if sensors_reload_flag.is_set():
                print("\n" + "="*70)
                print("[RELOAD] Sensor configuration changed - reloading sensors...")
                print("="*70)
                sensors_list = fetch_connected_sensors(mongo_client)
                sensors_reload_flag.clear()
                print(f"[RELOAD] Complete - now processing {len(sensors_list)} sensors\n")
            
            # If no sensors, wait and check again
            if not sensors_list:
                print("[WARN] No connected sensors available. Waiting for sensor changes...")
                time.sleep(INSERT_INTERVAL_SECONDS)
                continue
            
            cycle_start = datetime.datetime.now(pytz.UTC)
            print(f"\n[Cycle] Starting at {cycle_start.isoformat()}")
            
            # NEW: Fetch active alerts at the start of each cycle
            active_alerts = fetch_active_alerts(mongo_client)
            active_rules = fetch_active_rules(mongo_client)
            
            # Process each connected sensor
            for sensor_info in sensors_list:
                sensor_type = sensor_info['sensor_type']
                sensor_id = sensor_info['sensor_id']
                unique_id = sensor_info['unique_id']
                owner_id = sensor_info['owner_id']
                
                # Skip if sensor type not supported
                if sensor_type not in SENSOR_TYPE_GENERATORS:
                    print(f"  ⊗ Unsupported sensor type '{sensor_type}' for {unique_id} (ID: {sensor_id})")
                    continue
                
                try:
                    # Get appropriate database connection for this owner
                    db_conn = get_db_connection(owner_id, mongo_client)
                    
                    # Get num_toilets from restroom (using sensor's restroomId)
                    try:
                        restroom_obj_id = ObjectId(sensor_info['restroom_id'])
                        num_toilets = get_num_toilets(mongo_client, restroom_obj_id)
                    except Exception:
                        num_toilets = 4
                    
                    # Generate dummy data using the appropriate generator
                    generator_func = SENSOR_TYPE_GENERATORS[sensor_type]
                    data = generator_func(sensor_info, num_toilets)
                    
                    # Insert data using the appropriate inserter
                    inserter_func = SENSOR_TYPE_INSERTERS[sensor_type]
                    inserter_func(db_conn, data)
                    
                    # Determine which DB was used
                    db_type = "CustomDB" if owner_id in custom_db_cache and custom_db_cache[owner_id]["use_custom"] else "DefaultDB"
                    print(f"  ✓ [{db_type}] {sensor_type}: {unique_id} (sensorId: {sensor_id}, owner: {owner_id})")
                    
                    # NEW: Check alerts after inserting data
                    check_alerts_for_data(mongo_client, sensor_info, data, active_alerts)
                    check_rules_for_data(mongo_client, sensor_info, data, active_rules)
                    
                except Exception as e:
                    print(f"[ERROR] Failed to process sensor {unique_id} (ID: {sensor_id}, type: {sensor_type}, owner: {owner_id}): {e}")
                    traceback.print_exc()
            
            # Sleep until next cycle
            elapsed = (datetime.datetime.now(pytz.UTC) - cycle_start).total_seconds()
            sleep_for = max(0, INSERT_INTERVAL_SECONDS - elapsed)
            print(f"\n[Simulator] Cycle complete. Waiting {sleep_for:.1f}s before next cycle...")
            time.sleep(sleep_for)
    
    except KeyboardInterrupt:
        print("\n[Simulator] Interrupted by user. Exiting...")
        shutdown_flag.set()
    
    except Exception as ex:
        print("[FATAL] Exception in main_loop:", ex)
        traceback.print_exc()
        shutdown_flag.set()
    
    finally:
        # Cleanup
        if mongo_client:
            mongo_client.close()
        if default_mysql_conn:
            default_mysql_conn.close()
        
        # Close all custom DB connections
        with custom_db_cache_lock:
            for owner_id, cached in custom_db_cache.items():
                try:
                    if cached["use_custom"]:
                        cached["connection"].close()
                        print(f"[Cleanup] Closed custom DB connection for owner {owner_id}")
                except Exception:
                    pass
        
        print("[Simulator] Cleanup complete. Goodbye!")

if __name__ == "__main__":
    main_loop()