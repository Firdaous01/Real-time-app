import time
import csv
import psycopg2
from psycopg2 import sql
import os
from urllib.parse import urlparse

# Utiliser la chaîne de connexion fournie
DATABASE_URL = "postgres://postgres:root@localhost:5432/smartphone_db"

# Fonction pour établir une connexion à la base de données PostgreSQL en utilisant DATABASE_URL
def connect_to_db():
    try:
        # Analyser l'URL de connexion
        result = urlparse(DATABASE_URL)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port
        
        # Établir la connexion
        conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )
        print("Connexion à la base de données réussie!")
        return conn
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données: {e}")
        return None

# Fonction pour créer la table si elle n'existe pas
def create_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS smartphones (
        id SERIAL PRIMARY KEY,
        brand VARCHAR(255),
        screen_size FLOAT,
        ram FLOAT,
        rom FLOAT,
        sim_type VARCHAR(255),
        battery FLOAT,
        price FLOAT
    );
    '''
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("Table 'smartphones' créée ou déjà existante.")
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la création de la table: {e}")

# Fonction pour valider et convertir les données
def validate_data(row):
    try:
        # Supposant que l'ordre des colonnes est: id, brand, screen_size, ram, rom, sim_type, battery, price
        validated_row = [
            int(row[0]),                    # id (int)
            str(row[1]),                    # brand (str)
            float(row[2]) if row[2] else None,  # screen_size (float)
            float(row[3]) if row[3] else None,  # ram (float)
            float(row[4]) if row[4] else None,  # rom (float)
            str(row[5]),                    # sim_type (str)
            float(row[6]) if row[6] else None,  # battery (float)
            float(row[7]) if row[7] else None   # price (float)
        ]
        return validated_row
    except (ValueError, IndexError) as e:
        print(f"Erreur de validation des données: {e} pour la ligne: {row}")
        return None

# Fonction pour insérer une ligne dans la base de données
def insert_data(conn, row):
    insert_query = '''
    INSERT INTO smartphones (id, brand, screen_size, ram, rom, sim_type, battery, price)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        brand = EXCLUDED.brand,
        screen_size = EXCLUDED.screen_size,
        ram = EXCLUDED.ram,
        rom = EXCLUDED.rom,
        sim_type = EXCLUDED.sim_type,
        battery = EXCLUDED.battery,
        price = EXCLUDED.price;
    '''
    try:
        validated_row = validate_data(row)
        if validated_row:
            with conn.cursor() as cur:
                cur.execute(insert_query, validated_row)
                conn.commit()
                print(f"Ligne insérée avec succès: {validated_row}")
                return True
        return False
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de l'insertion des données: {e}")
        return False

# Fonction pour lire les données du fichier CSV
def read_csv(file_path):
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Lire l'en-tête
            print(f"En-tête CSV: {header}")
            
            for row in csv_reader:
                yield row
    except FileNotFoundError:
        print(f"Erreur: Le fichier '{file_path}' n'existe pas.")
        yield from []
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier CSV: {e}")
        yield from []

def main():
    csv_file_path = 'smartphone_Data.csv'  # Chemin vers votre fichier CSV
    
    # Se connecter à la base de données
    conn = connect_to_db()
    if conn is None:
        print("Échec de la connexion à la base de données.")
        return

    # Créer la table si elle n'existe pas
    create_table(conn)
    
    # Compteurs pour les statistiques
    total_rows = 0
    successful_inserts = 0
    
    # Lire les données du CSV et les insérer dans la BD toutes les 5 secondes
    try:
        for row in read_csv(csv_file_path):
            total_rows += 1
            print(f"Traitement de la ligne {total_rows}: {row}")
            
            if insert_data(conn, row):
                successful_inserts += 1
            
            print(f"Attente de 5 secondes avant la prochaine insertion...")
            time.sleep(5)  # Attendre 5 secondes avant d'insérer la ligne suivante
    
    except KeyboardInterrupt:
        print("\nOpération interrompue par l'utilisateur.")
    finally:
        # Afficher les statistiques
        print(f"\nStatistiques d'importation:")
        print(f"Total des lignes traitées: {total_rows}")
        print(f"Insertions réussies: {successful_inserts}")
        print(f"Échecs: {total_rows - successful_inserts}")
        
        # Fermer la connexion à la base de données
        if conn:
            conn.close()
            print("Connexion à la base de données fermée.")

if __name__ == '__main__':
    main()