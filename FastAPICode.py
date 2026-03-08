from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uvicorn
import json
import os

# Initialize FastAPI app
app = FastAPI(
    title="Todo Application API",
    description="A complete todo management application",
    version="1.0.0"
)

# Define Pydantic models
class TodoBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Todo title")
    description: Optional[str] = Field(None, max_length=1000, description="Detailed description")
    completed: bool = Field(False, description="Completion status")
    priority: Optional[str] = Field("medium", pattern="^(low|medium|high)$", description="Priority level")

class Todo(TodoBase):
    id: int = Field(..., description="Unique todo ID")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

class TodoCreate(TodoBase):
    pass

class TodoUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    completed: Optional[bool] = None
    priority: Optional[str] = Field(None, pattern="^(low|medium|high)$")

# Database file
DB_FILE = "todos.json"

# Load todos from file
def load_todos():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                data = json.load(f)
                return {int(k): v for k, v in data.items()}
        except Exception:
            return {}
    return {}

# Save todos to file
def save_todos(todos_dict):
    with open(DB_FILE, "w") as f:
        json.dump({str(k): v for k, v in todos_dict.items()}, f, indent=2, default=str)

# Initialize in-memory database
todos_db = load_todos()
next_id = max(todos_db.keys()) + 1 if todos_db else 1

# Root endpoint
@app.get("/")
async def read_root():
    return {
        "message": "Welcome to Todo Application API",
        "status": "running",
        "version": "1.0.0",
        "docs_url": "/docs"
    }

# GET all todos with optional filtering
@app.get("/todos/", response_model=List[Todo])
async def get_todos(
    completed: Optional[bool] = None,
    priority: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieve all todos with optional filters for completion status and priority.
    - **completed**: Filter by completion status (true/false)
    - **priority**: Filter by priority (low/medium/high)
    - **skip**: Number of todos to skip
    - **limit**: Maximum number of todos to return
    """
    filtered_todos = []
    
    for todo_data in todos_db.values():
        # Apply filters
        if completed is not None and todo_data.get("completed") != completed:
            continue
        if priority is not None and todo_data.get("priority") != priority:
            continue
        filtered_todos.append(todo_data)
    
    # Apply pagination
    return filtered_todos[skip:skip + limit]

# GET a specific todo by ID
@app.get("/todos/{todo_id}", response_model=Todo)
async def get_todo(todo_id: int):
    """Retrieve a specific todo by ID."""
    if todo_id not in todos_db:
        raise HTTPException(status_code=404, detail="Todo not found")
    return todos_db[todo_id]

# POST create a new todo
@app.post("/todos/", response_model=Todo, status_code=201)
async def create_todo(todo: TodoCreate):
    """Create a new todo item."""
    global next_id
    
    new_todo = {
        "id": next_id,
        "title": todo.title,
        "description": todo.description,
        "completed": todo.completed,
        "priority": todo.priority,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    todos_db[next_id] = new_todo
    next_id += 1
    save_todos(todos_db)
    
    return new_todo

# PUT update a todo
@app.put("/todos/{todo_id}", response_model=Todo)
async def update_todo(todo_id: int, todo_update: TodoUpdate):
    """Update an existing todo."""
    if todo_id not in todos_db:
        raise HTTPException(status_code=404, detail="Todo not found")
    
    existing_todo = todos_db[todo_id]
    
    # Update only provided fields
    if todo_update.title is not None:
        existing_todo["title"] = todo_update.title
    if todo_update.description is not None:
        existing_todo["description"] = todo_update.description
    if todo_update.completed is not None:
        existing_todo["completed"] = todo_update.completed
    if todo_update.priority is not None:
        existing_todo["priority"] = todo_update.priority
    
    existing_todo["updated_at"] = datetime.now().isoformat()
    todos_db[todo_id] = existing_todo
    save_todos(todos_db)
    
    return existing_todo

# PATCH mark todo as completed
@app.patch("/todos/{todo_id}/complete", response_model=Todo)
async def complete_todo(todo_id: int):
    """Mark a todo as completed."""
    if todo_id not in todos_db:
        raise HTTPException(status_code=404, detail="Todo not found")
    
    todos_db[todo_id]["completed"] = True
    todos_db[todo_id]["updated_at"] = datetime.now().isoformat()
    save_todos(todos_db)
    
    return todos_db[todo_id]

# DELETE a todo
@app.delete("/todos/{todo_id}", status_code=204)
async def delete_todo(todo_id: int):
    """Delete a todo by ID."""
    if todo_id not in todos_db:
        raise HTTPException(status_code=404, detail="Todo not found")
    
    todos_db.pop(todo_id)
    save_todos(todos_db)
    
    return None

# DELETE all todos
@app.delete("/todos/", status_code=204)
async def delete_all_todos(confirm: bool = False):
    """Delete all todos (requires confirmation)."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Add ?confirm=true to delete all todos"
        )
    
    todos_db.clear()
    save_todos(todos_db)
    
    return None

# GET statistics
@app.get("/todos/stats/summary")
async def get_stats():
    """Get todo statistics."""
    total = len(todos_db)
    completed = sum(1 for t in todos_db.values() if t.get("completed"))
    pending = total - completed
    
    by_priority = {
        "low": sum(1 for t in todos_db.values() if t.get("priority") == "low"),
        "medium": sum(1 for t in todos_db.values() if t.get("priority") == "medium"),
        "high": sum(1 for t in todos_db.values() if t.get("priority") == "high")
    }
    
    return {
        "total_todos": total,
        "completed": completed,
        "pending": pending,
        "completion_percentage": (completed / total * 100) if total > 0 else 0,
        "by_priority": by_priority
    }

# Health check endpoint
@app.get("/health/")
async def health_check():
    """Check API health status."""
    return {
        "status": "healthy",
        "total_todos": len(todos_db),
        "timestamp": datetime.now().isoformat()
    }

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)













# #!/usr/bin/env python3
# """
# Restroom sensor simulator with real-time MongoDB change detection and custom DB support
# - Connects to MongoDB to fetch ownerId, buildingId, restroomId from sensors collection
# - Creates per-sensor MySQL tables (CREATE TABLE IF NOT EXISTS)
# - Every 30 seconds generates realistic dummy values for each sensor type
# - Uses the actual sensor document data (restroomId, buildingId, sensorId) from MongoDB
# - Only processes sensors where isConnected=true
# - Automatically detects changes in MongoDB sensors collection and reloads data
# - Supports custom database routing per owner (checks auths collection)
# - Uses pymongo and pymysql
# """

# import time
# import random
# import datetime
# import traceback
# import threading
# from typing import Dict, Any, List, Optional
# import pytz
# from pymongo import MongoClient
# from bson import ObjectId
# import pymysql

# # ---------------------------
# # Configuration (edit if needed)
# # ---------------------------

# # MongoDB (as provided)
# MONGO_URI = "mongodb+srv://hamzajani:hamzamongo.55@cluster0.n92vrzt.mongodb.net/rest-room"
# MONGO_DB_NAME = "rest-room"
# MONGO_RESTROOM_COLLECTION = "restrooms"
# MONGO_SENSOR_COLLECTION = "sensors"
# MONGO_AUTH_COLLECTION = "auths"

# # MySQL (as provided) - Default DB
# SQL_DB_NAME = "final-test-restrrom"
# SQL_HOST_NAME = "5.223.51.13"
# SQL_USERNAME = "root"
# SQL_PASSWORD = "security890"
# SQL_PORT = 3306

# # Insert interval (30 seconds)
# INSERT_INTERVAL_SECONDS = 30

# # Flag to signal reload of sensors
# sensors_reload_flag = threading.Event()
# shutdown_flag = threading.Event()

# # Cache for custom DB connections and configurations
# custom_db_cache = {}
# custom_db_cache_lock = threading.Lock()

# # ---------------------------
# # MongoDB helpers
# # ---------------------------

# def connect_mongo(uri: str = MONGO_URI) -> MongoClient:
#     client = MongoClient(uri, serverSelectionTimeoutMS=10000)
#     client.server_info()
#     return client

# def fetch_owner_db_config(mongo_client: MongoClient, owner_id: str, db_name: str = MONGO_DB_NAME, auth_collection: str = MONGO_AUTH_COLLECTION) -> Optional[Dict[str, Any]]:
#     """
#     Fetch custom database configuration for a specific owner from auths collection.
#     Returns None if owner not found or custom DB not configured.
#     """
#     try:
#         db = mongo_client[db_name]
#         owner_obj_id = ObjectId(owner_id)
#         auth_doc = db[auth_collection].find_one({"_id": owner_obj_id})
        
#         if not auth_doc:
#             return None
        
#         is_custom_db = auth_doc.get("isCustomDb", False)
        
#         if not is_custom_db:
#             return None
        
#         # Extract custom DB configuration
#         custom_config = {
#             "host": auth_doc.get("customDbHost"),
#             "username": auth_doc.get("customDbUsername"),
#             "password": auth_doc.get("customDbPassword"),
#             "database": auth_doc.get("customDbName"),
#             "port": auth_doc.get("customDbPort", 3306),
#             "is_connected": auth_doc.get("isCustomDbConnected", False)
#         }
        
#         # Validate that all required fields are present
#         if all([custom_config["host"], custom_config["username"], 
#                 custom_config["password"], custom_config["database"]]):
#             return custom_config
#         else:
#             print(f"[WARN] Owner {owner_id} has isCustomDb=true but incomplete configuration")
#             return None
            
#     except Exception as e:
#         print(f"[ERROR] Failed to fetch owner DB config for {owner_id}: {e}")
#         return None

# def fetch_connected_sensors(mongo_client: MongoClient, 
#                            db_name: str = MONGO_DB_NAME, 
#                            sensor_collection: str = MONGO_SENSOR_COLLECTION) -> List[Dict[str, Any]]:
#     """
#     Fetch all connected sensors from MongoDB where isConnected=true.
#     Returns a list of sensor info dictionaries.
#     """
#     db = mongo_client[db_name]
    
#     # Fetch all sensors where isConnected=true
#     sensor_docs = list(db[sensor_collection].find({"isConnected": True}))
    
#     if not sensor_docs:
#         print(f"[WARN] No connected sensors found in MongoDB collection '{sensor_collection}' (isConnected=true)")
#         return []
    
#     # Build sensors list with all required information
#     sensors_list = []
#     for sensor_doc in sensor_docs:
#         sensor_id = str(sensor_doc.get("_id"))
#         owner_id = str(sensor_doc.get("ownerId", ""))
#         building_id = str(sensor_doc.get("buildingId", ""))
#         restroom_id = str(sensor_doc.get("restroomId", ""))
#         sensor_type = sensor_doc.get("sensorType", "")
#         unique_id = sensor_doc.get("uniqueId", "")
#         is_connected = sensor_doc.get("isConnected", False)
        
#         if is_connected and sensor_type:
#             sensors_list.append({
#                 "sensor_id": sensor_id,
#                 "owner_id": owner_id,
#                 "building_id": building_id,
#                 "restroom_id": restroom_id,
#                 "sensor_type": sensor_type,
#                 "unique_id": unique_id,
#                 "is_connected": is_connected
#             })
#             print(f"[MongoDB] Loaded sensor: {unique_id} (type: {sensor_type}, id: {sensor_id}, owner: {owner_id})")
#         else:
#             print(f"[WARN] Skipping sensor {sensor_id} - isConnected={is_connected} or missing sensorType")
    
#     print(f"[MongoDB] Total connected sensors loaded: {len(sensors_list)}")
#     return sensors_list

# def get_num_toilets(mongo_client: MongoClient, restroom_id: str, 
#                     db_name: str = MONGO_DB_NAME, 
#                     restroom_collection: str = MONGO_RESTROOM_COLLECTION) -> int:
#     """
#     Fetch numOfToilets from restroom document.
#     """
#     db = mongo_client[db_name]
#     rest_doc = db[restroom_collection].find_one({"_id": restroom_id})
#     if rest_doc:
#         try:
#             return int(rest_doc.get("numOfToilets", 4))
#         except Exception:
#             return 4
#     return 4

# def monitor_sensor_changes(mongo_client: MongoClient, db_name: str = MONGO_DB_NAME):
#     """
#     Monitor entire MongoDB database for changes using change streams.
#     Sets the reload flag when changes are detected in any collection.
#     """
#     print("[ChangeStream] Starting MongoDB change stream monitor...")
#     try:
#         db = mongo_client[db_name]
#         # Watch for insert, update, delete, and replace operations on any collection
#         with db.watch([
#             {'$match': {
#                 'operationType': {'$in': ['insert', 'update', 'delete', 'replace']}
#             }}
#         ]) as stream:
#             print("[ChangeStream] Monitoring entire database for changes...")
#             for change in stream:
#                 if shutdown_flag.is_set():
#                     break
                
#                 operation = change.get('operationType', 'unknown')
#                 doc_id = change.get('documentKey', {}).get('_id', 'unknown')
#                 collection = change.get('ns', {}).get('coll', 'unknown')
                
#                 print(f"\n[ChangeStream] Detected {operation} operation in collection '{collection}' on document {doc_id}")
#                 print("[ChangeStream] Triggering sensor reload...")
                
#                 # Clear custom DB cache when auths collection changes
#                 if collection == MONGO_AUTH_COLLECTION:
#                     with custom_db_cache_lock:
#                         custom_db_cache.clear()
#                         print("[ChangeStream] Cleared custom DB cache due to auths collection change")
                
#                 # Set the reload flag
#                 sensors_reload_flag.set()
                
#     except Exception as e:
#         if not shutdown_flag.is_set():
#             print(f"[ChangeStream ERROR] {e}")
#             print("[ChangeStream] Will retry monitoring in 10 seconds...")
#             time.sleep(10)
#             if not shutdown_flag.is_set():
#                 monitor_sensor_changes(mongo_client, db_name)

# # ---------------------------
# # MySQL helpers
# # ---------------------------

# def connect_mysql(host=SQL_HOST_NAME, user=SQL_USERNAME, password=SQL_PASSWORD, 
#                  db=SQL_DB_NAME, port=SQL_PORT):
#     conn = pymysql.connect(
#         host=host,
#         user=user,
#         password=password,
#         database=db,
#         port=port,
#         charset='utf8mb4',
#         cursorclass=pymysql.cursors.DictCursor,
#         autocommit=False
#     )
#     return conn

# def get_db_connection(owner_id: str, mongo_client: MongoClient) -> pymysql.connections.Connection:
#     """
#     Get appropriate database connection for the owner.
#     Returns custom DB connection if configured, otherwise default DB connection.
#     """
#     # Check cache first
#     with custom_db_cache_lock:
#         if owner_id in custom_db_cache:
#             cached = custom_db_cache[owner_id]
#             if cached["use_custom"]:
#                 # Try to use cached connection
#                 try:
#                     cached["connection"].ping(reconnect=True)
#                     return cached["connection"]
#                 except Exception:
#                     # Connection dead, remove from cache
#                     del custom_db_cache[owner_id]
#             else:
#                 # Use default connection
#                 return cached["connection"]
    
#     # Fetch owner's DB configuration
#     custom_config = fetch_owner_db_config(mongo_client, owner_id)
    
#     if custom_config:
#         # Try to connect to custom database
#         try:
#             print(f"[CustomDB] Connecting to custom DB for owner {owner_id}: {custom_config['host']}:{custom_config['port']}/{custom_config['database']}")
#             custom_conn = connect_mysql(
#                 host=custom_config["host"],
#                 user=custom_config["username"],
#                 password=custom_config["password"],
#                 db=custom_config["database"],
#                 port=custom_config["port"]
#             )
            
#             # Create tables in custom DB
#             create_tables(custom_conn)
            
#             # Cache the connection
#             with custom_db_cache_lock:
#                 custom_db_cache[owner_id] = {
#                     "use_custom": True,
#                     "connection": custom_conn,
#                     "config": custom_config
#                 }
            
#             print(f"[CustomDB] Successfully connected to custom DB for owner {owner_id}")
#             return custom_conn
            
#         except Exception as e:
#             print(f"[CustomDB ERROR] Failed to connect to custom DB for owner {owner_id}: {e}")
#             print(f"[CustomDB] Falling back to default DB for owner {owner_id}")
    
#     # Use default connection
#     default_conn = connect_mysql()
    
#     # Cache the default connection decision
#     with custom_db_cache_lock:
#         custom_db_cache[owner_id] = {
#             "use_custom": False,
#             "connection": default_conn,
#             "config": None
#         }
    
#     return default_conn

# def create_tables(conn):
#     """
#     Create all sensor tables (CREATE TABLE IF NOT EXISTS)
#     """
#     with conn.cursor() as cur:
#         # Door Queue Sensor
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS door_queue (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 event VARCHAR(16),
#                 `count` INT,
#                 queueState VARCHAR(32),
#                 windowCount INT
#             ) ENGINE=InnoDB;
#         """)
        
#         # Stall / Door Status
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS stall_status (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 `state` VARCHAR(32),
#                 usageCount INT
#             ) ENGINE=InnoDB;
#         """)
        
#         # Occupancy Sensor
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS occupancy (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 occupied BOOLEAN,
#                 occupancyDuration INT,
#                 lastOccupiedAt DATETIME
#             ) ENGINE=InnoDB;
#         """)
        
#         # Air Quality Sensor
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS air_quality (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 tvoc FLOAT,
#                 eCO2 INT,
#                 pm2_5 FLOAT,
#                 aqi INT,
#                 smellLevel VARCHAR(32)
#             ) ENGINE=InnoDB;
#         """)
        
#         # Toilet Paper Level
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS toilet_paper (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 `level` INT,
#                 `status` VARCHAR(32),
#                 lastRefilledAt DATETIME
#             ) ENGINE=InnoDB;
#         """)
        
#         # Handwash Sensor
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS handwash (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 dispenseEvent VARCHAR(32),
#                 lastDispenseAt DATETIME,
#                 `level` INT,
#                 `status` VARCHAR(32)
#             ) ENGINE=InnoDB;
#         """)
        
#         # Soap Dispenser
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS soap_dispenser (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 dispenseEvent VARCHAR(32),
#                 lastDispenseAt DATETIME,
#                 `level` INT,
#                 `status` VARCHAR(32)
#             ) ENGINE=InnoDB;
#         """)
        
#         # Water Leakage
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS water_leakage (
#                 idPrimary BIGINT PRIMARY KEY AUTO_INCREMENT,
#                 ownerId VARCHAR(128),
#                 buildingId VARCHAR(128),
#                 restroomId VARCHAR(128),
#                 stallId VARCHAR(64),
#                 sensorId VARCHAR(128),
#                 sensor_unique_id VARCHAR(128),
#                 `timestamp` DATETIME,
#                 waterDetected BOOLEAN,
#                 waterLevel_mm FLOAT
#             ) ENGINE=InnoDB;
#         """)
    
#     conn.commit()
#     print("[MySQL] Created tables (if not existed)")

# # ---------------------------
# # Helper functions
# # ---------------------------

# def now_dt():
#     # Return current time in UTC (converted from Jeddah time)
#     tz = pytz.timezone('Asia/Riyadh')
#     dt_local = datetime.datetime.now(tz)
#     return dt_local.astimezone(pytz.UTC).replace(tzinfo=None)

# def choose_stall(num_toilets: int) -> str:
#     if not num_toilets or num_toilets <= 0:
#         return "stall-1"
#     return f"stall-{random.randint(1, max(1, num_toilets))}"

# # ---------------------------
# # Dummy value generators (one per sensor type)
# # ---------------------------

# def generate_dummy_door_queue(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     event = random.choices(["enter", "exit", "none"], weights=[25, 25, 50])[0]
#     count = random.randint(5, 25) if event != "none" else random.randint(0, 8)
    
#     if count == 0:
#         queue_state = "idle"
#     elif count < 10:
#         queue_state = "forming"
#     else:
#         queue_state = "full"
    
#     window_count = max(0, count + random.randint(-3, 5))
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         event=event,
#         count=count,
#         queueState=queue_state,
#         windowCount=window_count
#     )

# def generate_dummy_stall_status(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     state = random.choice(["open", "occupied", "locked", "vacant"])
#     usageCount = random.randint(50, 500)
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         state=state,
#         usageCount=usageCount
#     )

# def generate_dummy_occupancy(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     occupied = random.choice([True, False])
#     occupancyDuration = random.randint(30, 1800) if occupied else 0
    
#     if not occupied:
#         lastOccupiedAt = ts - datetime.timedelta(seconds=random.randint(60, 7200))
#     else:
#         lastOccupiedAt = ts - datetime.timedelta(seconds=random.randint(1, occupancyDuration))
    
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         occupied=occupied,
#         occupancyDuration=occupancyDuration,
#         lastOccupiedAt=lastOccupiedAt
#     )

# def generate_dummy_air_quality(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     tvoc = round(random.uniform(200.0, 2500.0), 2)
#     eCO2 = random.randint(800, 3000)
#     pm2_5 = round(random.uniform(10.0, 150.0), 2)
    
#     if pm2_5 <= 12:
#         aqi = random.randint(0, 50)
#     elif pm2_5 <= 35.4:
#         aqi = random.randint(51, 100)
#     elif pm2_5 <= 55.4:
#         aqi = random.randint(101, 150)
#     else:
#         aqi = random.randint(151, 300)
    
#     smellLevel = random.choice(["none", "slight", "moderate", "strong"])
#     stallId = "common"
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         tvoc=tvoc,
#         eCO2=eCO2,
#         pm2_5=pm2_5,
#         aqi=aqi,
#         smellLevel=smellLevel
#     )

# def generate_dummy_toilet_paper(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     level = random.randint(10, 100)
#     status = "ok" if level > 30 else "low"
#     lastRefilledAt = ts - datetime.timedelta(hours=random.randint(1, 48)) if random.random() > 0.05 else ts
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         level=level,
#         status=status,
#         lastRefilledAt=lastRefilledAt
#     )

# def generate_dummy_handwash(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     dispenseEvent = random.choice(["dispense", "none", "dispense", "dispense"])
    
#     if dispenseEvent == "dispense":
#         lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(30, 1800))
#     else:
#         lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(3600, 14400))
    
#     level = random.randint(20, 100)
#     status = "ok" if level > 25 else "refill_required"
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         dispenseEvent=dispenseEvent,
#         lastDispenseAt=lastDispenseAt,
#         level=level,
#         status=status
#     )

# def generate_dummy_soap_dispenser(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     dispenseEvent = random.choice(["dispense", "none", "dispense", "dispense"])
    
#     if dispenseEvent == "dispense":
#         lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(30, 1800))
#     else:
#         lastDispenseAt = ts - datetime.timedelta(seconds=random.randint(3600, 14400))
    
#     level = random.randint(20, 100)
#     status = "ok" if level > 30 else "refill_required"
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         dispenseEvent=dispenseEvent,
#         lastDispenseAt=lastDispenseAt,
#         level=level,
#         status=status
#     )

# def generate_dummy_water_leakage(sensor_info: Dict[str, Any], num_toilets: int) -> Dict[str, Any]:
#     ts = now_dt()
#     waterDetected = random.choices([False, True], weights=[85, 15])[0]
#     waterLevel_mm = round(random.uniform(10.0, 250.0), 1)
#     stallId = choose_stall(num_toilets)
    
#     return dict(
#         ownerId=sensor_info['owner_id'],
#         buildingId=sensor_info['building_id'],
#         restroomId=sensor_info['restroom_id'],
#         stallId=stallId,
#         sensorId=sensor_info['sensor_id'],
#         sensor_unique_id=sensor_info['unique_id'],
#         timestamp=ts,
#         waterDetected=waterDetected,
#         waterLevel_mm=waterLevel_mm
#     )

# # ---------------------------
# # MySQL insert functions
# # ---------------------------

# def insert_door_queue(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO door_queue 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, event, `count`, queueState, windowCount)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['event'],
#               data['count'], data['queueState'], data['windowCount']))
#     conn.commit()

# def insert_stall_status(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO stall_status 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, `state`, usageCount)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['state'],
#               data['usageCount']))
#     conn.commit()

# def insert_occupancy(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO occupancy 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, occupied, occupancyDuration, lastOccupiedAt)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
#               int(data['occupied']), data['occupancyDuration'], data['lastOccupiedAt']))
#     conn.commit()

# def insert_air_quality(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO air_quality 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, tvoc, eCO2, pm2_5, aqi, smellLevel)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['tvoc'],
#               data['eCO2'], data['pm2_5'], data['aqi'], data['smellLevel']))
#     conn.commit()

# def insert_toilet_paper(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO toilet_paper 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, `level`, `status`, lastRefilledAt)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], data['level'],
#               data['status'], data['lastRefilledAt']))
#     conn.commit()

# def insert_handwash(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO handwash 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, dispenseEvent, lastDispenseAt, `level`, `status`)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
#               data['dispenseEvent'], data['lastDispenseAt'], data['level'], data['status']))
#     conn.commit()

# def insert_soap_dispenser(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO soap_dispenser 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, dispenseEvent, lastDispenseAt, `level`, `status`)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
#               data['dispenseEvent'], data['lastDispenseAt'], data['level'], data['status']))
#     conn.commit()

# def insert_water_leakage(conn, data: Dict[str, Any]):
#     with conn.cursor() as cur:
#         cur.execute("""
#             INSERT INTO water_leakage 
#             (ownerId, buildingId, restroomId, stallId, sensorId, sensor_unique_id, 
#              `timestamp`, waterDetected, waterLevel_mm)
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (data['ownerId'], data['buildingId'], data['restroomId'], data['stallId'],
#               data['sensorId'], data['sensor_unique_id'], data['timestamp'], 
#               int(data['waterDetected']), data['waterLevel_mm']))
#     conn.commit()

# # ---------------------------
# # Sensor type mapping
# # ---------------------------

# SENSOR_TYPE_GENERATORS = {
#     "door_queue": generate_dummy_door_queue,
#     "stall_status": generate_dummy_stall_status,
#     "occupancy": generate_dummy_occupancy,
#     "air_quality": generate_dummy_air_quality,
#     "toilet_paper": generate_dummy_toilet_paper,
#     "handwash": generate_dummy_handwash,
#     "soap_dispenser": generate_dummy_soap_dispenser,
#     "water_leakage": generate_dummy_water_leakage,
# }

# SENSOR_TYPE_INSERTERS = {
#     "door_queue": insert_door_queue,
#     "stall_status": insert_stall_status,
#     "occupancy": insert_occupancy,
#     "air_quality": insert_air_quality,
#     "toilet_paper": insert_toilet_paper,
#     "handwash": insert_handwash,
#     "soap_dispenser": insert_soap_dispenser,
#     "water_leakage": insert_water_leakage,
# }

# # ---------------------------
# # Main loop
# # ---------------------------

# def main_loop():
#     mongo_client = None
#     default_mysql_conn = None
#     change_stream_thread = None
    
#     try:
#         # Connect to MongoDB
#         print("[MongoDB] Connecting...")
#         mongo_client = connect_mongo()
        
#         # Connect to default MySQL and create tables once
#         print("[MySQL] Connecting to default database...")
#         default_mysql_conn = connect_mysql()
#         create_tables(default_mysql_conn)
        
#         # Start change stream monitor in separate thread
#         change_stream_thread = threading.Thread(
#             target=monitor_sensor_changes,
#             args=(mongo_client,),
#             daemon=True
#         )
#         change_stream_thread.start()
        
#         # Initial sensor load
#         sensors_list = fetch_connected_sensors(mongo_client)
#         print(f"\n[Simulator] Starting main loop - processing {len(sensors_list)} connected sensors")
#         print("[Simulator] Custom DB routing enabled - checking auths collection per owner")
#         print("[Simulator] Press Ctrl+C to stop\n")
        
#         while True:
#             # Check if reload is needed
#             if sensors_reload_flag.is_set():
#                 print("\n" + "="*70)
#                 print("[RELOAD] Sensor configuration changed - reloading sensors...")
#                 print("="*70)
#                 sensors_list = fetch_connected_sensors(mongo_client)
#                 sensors_reload_flag.clear()
#                 print(f"[RELOAD] Complete - now processing {len(sensors_list)} sensors\n")
            
#             # If no sensors, wait and check again
#             if not sensors_list:
#                 print("[WARN] No connected sensors available. Waiting for sensor changes...")
#                 time.sleep(INSERT_INTERVAL_SECONDS)
#                 continue
            
#             cycle_start = datetime.datetime.utcnow()
#             print(f"\n[Cycle] Starting at {cycle_start.isoformat()}")
            
#             # Process each connected sensor
#             for sensor_info in sensors_list:
#                 sensor_type = sensor_info['sensor_type']
#                 sensor_id = sensor_info['sensor_id']
#                 unique_id = sensor_info['unique_id']
#                 owner_id = sensor_info['owner_id']
                
#                 # Skip if sensor type not supported
#                 if sensor_type not in SENSOR_TYPE_GENERATORS:
#                     print(f"  ⊗ Unsupported sensor type '{sensor_type}' for {unique_id} (ID: {sensor_id})")
#                     continue
                
#                 try:
#                     # Get appropriate database connection for this owner
#                     db_conn = get_db_connection(owner_id, mongo_client)
                    
#                     # Get num_toilets from restroom (using sensor's restroomId)
#                     try:
#                         restroom_obj_id = ObjectId(sensor_info['restroom_id'])
#                         num_toilets = get_num_toilets(mongo_client, restroom_obj_id)
#                     except Exception:
#                         num_toilets = 4
                    
#                     # Generate dummy data using the appropriate generator
#                     generator_func = SENSOR_TYPE_GENERATORS[sensor_type]
#                     data = generator_func(sensor_info, num_toilets)
                    
#                     # Insert data using the appropriate inserter
#                     inserter_func = SENSOR_TYPE_INSERTERS[sensor_type]
#                     inserter_func(db_conn, data)
                    
#                     # Determine which DB was used
#                     db_type = "CustomDB" if owner_id in custom_db_cache and custom_db_cache[owner_id]["use_custom"] else "DefaultDB"
#                     print(f"  ✓ [{db_type}] {sensor_type}: {unique_id} (sensorId: {sensor_id}, owner: {owner_id})")
                    
#                 except Exception as e:
#                     print(f"[ERROR] Failed to process sensor {unique_id} (ID: {sensor_id}, type: {sensor_type}, owner: {owner_id}): {e}")
#                     traceback.print_exc()
            
#             # Sleep until next cycle
#             elapsed = (datetime.datetime.utcnow() - cycle_start).total_seconds()
#             sleep_for = max(0, INSERT_INTERVAL_SECONDS - elapsed)
#             print(f"\n[Simulator] Cycle complete. Waiting {sleep_for:.1f}s before next cycle...")
#             time.sleep(sleep_for)
    
#     except KeyboardInterrupt:
#         print("\n[Simulator] Interrupted by user. Exiting...")
#         shutdown_flag.set()
    
#     except Exception as ex:
#         print("[FATAL] Exception in main_loop:", ex)
#         traceback.print_exc()
#         shutdown_flag.set()
    
#     finally:
#         # Cleanup
#         if mongo_client:
#             mongo_client.close()
#         if default_mysql_conn:
#             default_mysql_conn.close()
        
#         # Close all custom DB connections
#         with custom_db_cache_lock:
#             for owner_id, cached in custom_db_cache.items():
#                 try:
#                     if cached["use_custom"]:
#                         cached["connection"].close()
#                         print(f"[Cleanup] Closed custom DB connection for owner {owner_id}")
#                 except Exception:
#                     pass
        
#         print("[Simulator] Cleanup complete. Goodbye!")

# if __name__ == "__main__":
#     main_loop()


