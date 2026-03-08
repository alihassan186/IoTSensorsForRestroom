# #!/usr/bin/env python3
# """
# Simple MySQL Table Dropper - No confirmations, just drops everything
# """

# import pymysql

# # Configuration
# SQL_DB_NAME = "final-test-restrrom"
# SQL_HOST_NAME = "5.223.51.13"
# SQL_USERNAME = "root"
# SQL_PASSWORD = "security890"
# SQL_PORT = 3306

# # Tables to drop
# TABLES = [
#     "door_queue",
#     "stall_status",
#     "occupancy",
#     "air_quality",
#     "toilet_paper",
#     "handwash",
#     "soap_dispenser",
#     "water_leakage"
# ]

# def connect_db():
#     return pymysql.connect(
#         host=SQL_HOST_NAME,
#         user=SQL_USERNAME,
#         password=SQL_PASSWORD,
#         database=SQL_DB_NAME,
#         port=SQL_PORT,
#         connect_timeout=10,
#         read_timeout=20,
#         write_timeout=20,
#         autocommit=True
#     )


# def main():
#     print("Connecting to MySQL...")

#     conn = connect_db()
#     cursor = conn.cursor()

#     # Set a reasonable lock wait timeout and disable foreign key checks
#     try:
#         cursor.execute("SET SESSION innodb_lock_wait_timeout=50")
#     except Exception:
#         pass

#     try:
#         cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
#     except Exception as e:
#         print(f"Warning: could not disable foreign key checks: {e}")

#     print("\nDropping tables:\n")

#     for table in TABLES:
#         print(f"Attempting to drop: {table}...")
#         attempts = 0
#         while attempts < 2:
#             try:
#                 cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
#                 print(f"✓ Dropped: {table}")
#                 break
#             except pymysql.err.InterfaceError as ie:
#                 # Connection was lost; reconnect and retry once
#                 print(f"InterfaceError during DROP of {table}: {ie} — reconnecting and retrying")
#                 try:
#                     cursor.close()
#                 except Exception:
#                     pass
#                 try:
#                     conn.close()
#                 except Exception:
#                     pass
#                 conn = connect_db()
#                 cursor = conn.cursor()
#                 attempts += 1
#             except Exception as e:
#                 print(f"✗ Failed: {table} - {e}")
#                 # Diagnostics for locks/blocking sessions
#                 try:
#                     cursor.execute("SHOW FULL PROCESSLIST")
#                     rows = cursor.fetchall()
#                     print("Current process list (may show blocking threads):")
#                     for r in rows:
#                         print(r)
#                 except Exception as e2:
#                     print(f"Could not fetch processlist: {e2}")
#                 try:
#                     cursor.execute("""
#                     SELECT w.requesting_trx_id AS waiting_trx, r.trx_mysql_thread_id AS waiting_thread,
#                            w.blocking_trx_id AS blocking_trx, b.trx_mysql_thread_id AS blocking_thread
#                     FROM information_schema.innodb_lock_waits w
#                     JOIN information_schema.innodb_trx r ON w.requesting_trx_id = r.trx_id
#                     JOIN information_schema.innodb_trx b ON w.blocking_trx_id = b.trx_id;
#                     """)
#                     waits = cursor.fetchall()
#                     if waits:
#                         print("InnoDB lock waits (requesting -> blocking):")
#                         for w in waits:
#                             print(w)
#                 except Exception:
#                     pass
#                 break

#     print("\nRe-enabling foreign key checks...")
#     try:
#         cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
#     except pymysql.err.InterfaceError:
#         print("Connection lost when re-enabling foreign key checks, reconnecting to set it and close cleanly.")
#         try:
#             conn.close()
#         except Exception:
#             pass
#         try:
#             conn = connect_db()
#             cursor = conn.cursor()
#             cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
#         except Exception as e:
#             print(f"Could not re-enable foreign key checks: {e}")
#     except Exception as e:
#         print(f"Error re-enabling foreign key checks: {e}")

#     try:
#         conn.commit()
#     except Exception:
#         pass

#     try:
#         cursor.close()
#     except Exception:
#         pass

#     try:
#         conn.close()
#     except Exception:
#         pass

#     print("\n✅ DONE! All tables dropped.\n")

# if __name__ == "__main__":
#     main()


import pymysql

# Configuration
SQL_DB_NAME = "final-test-restrrom"
SQL_HOST_NAME = "5.223.51.13"
SQL_USERNAME = "root"
SQL_PASSWORD = "security890"
SQL_PORT = 3306

# Tables to alter
TABLES = [
    "door_queue",
    "stall_status",
    "occupancy",
    "air_quality",
    "toilet_paper",
    "handwash",
    "soap_dispenser",
    "water_leakage"
]

def alter_id_column():
    """Rename 'id' to 'idPrimary' in all specified tables."""
    try:
        conn = pymysql.connect(
            host=SQL_HOST_NAME,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DB_NAME,
            port=SQL_PORT
        )
        cursor = conn.cursor()
        
        print(f"Connected to database: {SQL_DB_NAME}")
        print("=" * 60)
        
        for table in TABLES:
            try:
                sql = f"ALTER TABLE {table} CHANGE COLUMN `id` `idPrimary` BIGINT NOT NULL AUTO_INCREMENT"
                cursor.execute(sql)
                conn.commit()
                print(f"✓ {table}: id → idPrimary")
            except pymysql.Error as e:
                print(f"✗ {table}: {str(e)}")
        
        print("=" * 60)
        print("Column rename completed!")
        
        cursor.close()
        conn.close()
        
    except pymysql.Error as e:
        print(f"Database connection error: {e}")

if __name__ == "__main__":
    alter_id_column()