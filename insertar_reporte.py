#!/usr/bin/env python3
import os
import json
import mysql.connector
from datetime import datetime

# Configuración de la base de datos
DB_HOST = "192.168.1.129"
DB_USER = "root"
DB_PASS = "Winner2020"
DB_NAME = "alcance"
TABLE_NAME = "reporte"

# Carpeta donde están los JSON generados por Ansible
RESULTS_DIR = "/connectivity_results"

def insert_data(fecha_ejecucion, host, status, msg):
    """Inserta un registro en la tabla reporte"""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    cursor = conn.cursor()

    sql = f"""
    INSERT INTO {TABLE_NAME} (fecha_ejecucion, host, status, msg)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (fecha_ejecucion, host, status, msg))

    conn.commit()
    cursor.close()
    conn.close()


def main():
    fecha_ejecucion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for filename in os.listdir(RESULTS_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(RESULTS_DIR, filename)
            with open(filepath, "r") as f:
                try:
                    data = json.load(f)
                    host = data.get("host", "desconocido")
                    status = data.get("status", "desconocido").strip()
                    msg = data.get("msg", "").strip()

                    insert_data(fecha_ejecucion, host, status, msg)
                    print(f"[OK] Insertado: {host} ({status})")
                except Exception as e:
                    print(f"[ERROR] No se pudo procesar {filename}: {e}")


if __name__ == "__main__":
    main()
