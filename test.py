import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import re
import psycopg2
from psycopg2 import sql

# Configuration initiale
load_dotenv()
pd.set_option('display.max_columns', None)

# Constantes
ID_PATTERN = r":\d+"
DATE_FORMATS = ["%m/%d/%y", "%d-%b-%y", "%Y-%m-%d"]
SELECTED_COLUMNS = [
    'id', 'country', 'shipment_mode', 'scheduled_delivery_date',
    'delivered_to_client_date', 'delivery_recorded_date',
    'product_group', 'sub_classification', 'vendor',
    'item_description', 'molecule_test_type', 'brand',
    'dosage', 'dosage_form', 'line_item_quantity',
    'line_item_value', 'unit_price', 'weight_kilograms',
    'freight_cost_usd', 'line_item_insurance_usd'
]

def load_csv_files(directory):
    """Charge tous les fichiers CSV d'un dossier dans une liste de DataFrames."""
    try:
        dataframes = [
            pd.read_csv(os.path.join(directory, file))
            for file in sorted(os.listdir(directory))
            if file.endswith(".csv")
        ]
        if not dataframes:
            raise ValueError("Aucun fichier CSV trouvé dans le dossier.")
        print(f"{len(dataframes)} fichiers CSV chargés.")
        return dataframes
    except FileNotFoundError:
        print(f"Erreur : Le dossier {directory} n'existe pas.")
        raise
    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture des fichiers CSV : {e}")
        raise

def clean_column_names(dataframes):
    """Nettoie les noms de colonnes des DataFrames en appliquant des transformations."""
    transformations = str.maketrans({
        '-': '_', ' ': '_', '$': '', '?': '', '/': '_', '\\': '_',
        '%': '', ')': '', '(': ''
    })
    for df in dataframes:
        df.columns = [col.lower().translate(transformations) for col in df.columns]
    return dataframes

def find_date_columns(df):
    """Identifie les colonnes contenant 'date' dans leur nom."""
    return [col for col in df.columns if "date" in col.lower()]

def parse_date(date_str):
    """Tente de convertir une chaîne en date selon plusieurs formats."""
    if pd.isna(date_str) or date_str in ['Pre-PQ Process', 'Date Not Captured', '']:
        return None  # Retourne None pour compatibilité avec PostgreSQL
    date_str = str(date_str).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return pd.to_datetime(date_str, errors='coerce')

def convert_date_columns(df):
    """Convertit les colonnes de date en format datetime."""
    date_columns = find_date_columns(df)
    if not date_columns:
        print("Aucune colonne de date trouvée dans le DataFrame.")
    for col in date_columns:
        df[col] = df[col].astype(str).str.strip().apply(parse_date)
    return df

def resolve_id_reference(value, dataset, column):
    """Remplace les références d'ID (ex: ':123') par la valeur correspondante dans dataset."""
    if not isinstance(value, str) or not re.match(ID_PATTERN, value):
        return value
    id_str = re.search(ID_PATTERN, value).group().replace(":", "")
    try:
        filtered = dataset.loc[dataset["id"] == int(id_str), column]
        return filtered.iloc[0] if not filtered.empty else value
    except (ValueError, IndexError):
        print(f"ID {id_str} non trouvé ou invalide pour la colonne {column}.")
        return value

def setup_postgres_database(
    db_name="scms_db",
    user="postgres",
    password=os.getenv("PG_PASSWORD"),
    host="localhost",
    port="5433"
):
    """Crée la base de données PostgreSQL si elle n'existe pas et retourne la connexion."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {} ENCODING 'UTF8'").format(
                sql.Identifier(db_name)
            ))
            print(f"Base de données '{db_name}' créée.")
        else:
            print(f"La base de données '{db_name}' existe déjà.")

        cursor.close()
        conn.close()

        pg_conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        pg_conn.autocommit = True
        print(f"Connexion établie à la base '{db_name}'.")
        return pg_conn
    except Exception as e:
        print(f"Erreur lors de la configuration de la base : {e}")
        raise

def create_table(pg_conn, table_name, df, foreign_keys=None):
    """Crée une table PostgreSQL avec des types adaptés et des contraintes de clés étrangères."""
    try:
        cursor = pg_conn.cursor()
        column_types = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT',
            'bool': 'BOOLEAN'
        }

        columns_def = []
        for col, dtype in df.dtypes.items():
            pg_type = column_types.get(str(dtype), 'TEXT')
            # Seule la colonne principale (delivery_id pour deliveries, *_id pour dimensions) est PRIMARY KEY
            if table_name == "deliveries" and col == "delivery_id":
                columns_def.append(f"{col} {pg_type} NOT NULL PRIMARY KEY")
            elif table_name != "deliveries" and col.endswith('_id'):
                columns_def.append(f"{col} {pg_type} NOT NULL PRIMARY KEY")
            else:
                columns_def.append(f"{col} {pg_type}")

        if foreign_keys:
            for fk in foreign_keys:
                columns_def.append(
                    f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']} ({fk['ref_column']})"
                )

        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.SQL, columns_def))
        )

        cursor.execute(create_table_query)
        print(f"Table '{table_name}' créée ou déjà existante.")
        cursor.close()
    except Exception as e:
        print(f"Erreur lors de la création de la table '{table_name}' : {e}")
        raise

def insert_data(pg_conn, table_name, df):
    """Insère les données du DataFrame dans la table PostgreSQL."""
    try:
        cursor = pg_conn.cursor()
        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(placeholders)
        )

        data = [
            tuple(None if pd.isna(val) else val for val in row)
            for row in df.itertuples(index=False, name=None)
        ]

        cursor.executemany(insert_query, data)
        print(f"{len(data)} lignes insérées dans la table '{table_name}'.")
        cursor.close()
    except Exception as e:
        print(f"Erreur lors de l'insertion des données dans '{table_name}' : {e}")
        raise

def create_dimension_tables(df):
    """Crée les DataFrames pour les tables de dimension avec des ID uniques."""
    # Table countries
    countries = df[["country"]].drop_duplicates().reset_index(drop=True)
    countries["country_id"] = countries.index + 1
    countries = countries.rename(columns={"country": "country_name"})

    # Table vendors
    vendors = df[["vendor"]].drop_duplicates().reset_index(drop=True)
    vendors["vendor_id"] = vendors.index + 1
    vendors = vendors.rename(columns={"vendor": "vendor_name"})

    # Table transport_modes
    transport_modes = df[["shipment_mode"]].drop_duplicates().reset_index(drop=True)
    transport_modes["mode_id"] = transport_modes.index + 1
    transport_modes = transport_modes.rename(columns={"shipment_mode": "mode_name"})

    # Table products
    products = df[["product_group", "sub_classification", "item_description",
                   "molecule_test_type", "brand", "dosage", "dosage_form"]].drop_duplicates().reset_index(drop=True)
    products["product_id"] = products.index + 1

    # Ajouter les ID dans le DataFrame principal
    df_with_ids = df.merge(countries, left_on="country", right_on="country_name", how="left")
    df_with_ids = df_with_ids.merge(vendors, left_on="vendor", right_on="vendor_name", how="left")
    df_with_ids = df_with_ids.merge(transport_modes, left_on="shipment_mode", right_on="mode_name", how="left")
    df_with_ids = df_with_ids.merge(products, on=["product_group", "sub_classification", "item_description",
                                                  "molecule_test_type", "brand", "dosage", "dosage_form"], how="left")

    # Renommer id en delivery_id et sélectionner les colonnes finales
    df_with_ids = df_with_ids.rename(columns={"id": "delivery_id"})
    final_columns = [
        'delivery_id', 'country_id', 'mode_id', 'scheduled_delivery_date',
        'delivered_to_client_date', 'delivery_recorded_date', 'product_id',
        'vendor_id', 'line_item_quantity', 'line_item_value', 'unit_price',
        'weight_kilograms', 'freight_cost_usd', 'line_item_insurance_usd'
    ]
    df_with_ids = df_with_ids[final_columns]

    return df_with_ids, countries, vendors, transport_modes, products

def display_head(df, title="Aperçu des données", n=5):
    """Affiche les premières lignes du DataFrame avec un titre."""
    print(f"\n=== {title} ===")
    print(df.head(n).to_string(index=False))
    print(f"Nombre total de lignes : {len(df)}")

def display_summary_stats(df, title="Résumé statistique"):
    """Affiche les statistiques descriptives des colonnes numériques."""
    print(f"\n=== {title} ===")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    print(df[numeric_cols].describe().to_string())

def display_value_counts(df, column, title="Répartition des valeurs"):
    """Affiche la répartition des valeurs d'une colonne catégorique."""
    print(f"\n=== {title} pour {column} ===")
    print(df[column].value_counts().to_string())
    print(f"Nombre de valeurs uniques : {df[column].nunique()}")


