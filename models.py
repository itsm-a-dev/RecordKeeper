from db import exec_safe, conn

def bootstrap_schema():
    exec_safe("""CREATE TABLE IF NOT EXISTS bets (...);""")
    exec_safe("""CREATE TABLE IF NOT EXISTS settings (...);""")
    exec_safe("""CREATE TABLE IF NOT EXISTS closings (...);""")
    exec_safe("""CREATE TABLE IF NOT EXISTS clv_fixes (...);""")
    conn.commit()
