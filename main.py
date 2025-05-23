from test import clean_column_names,load_csv_files,convert_date_columns,create_dimension_tables,SELECTED_COLUMNS,\
    resolve_id_reference,setup_postgres_database,create_table,insert_data,ID_PATTERN,display_head,display_summary_stats,display_value_counts

def main():
    """Pipeline principal pour traiter et ingérer les données dans PostgreSQL."""
    try:
        # Étape 1 : Chargement des données
        dataframes = load_csv_files("dataset")

        # Étape 2 : Nettoyage des noms de colonnes
        dataframes = clean_column_names(dataframes)

        # Étape 3 : Conversion des colonnes de date
        dataframes = [convert_date_columns(df) for df in dataframes]

        # Étape 4 : Sélection des colonnes pertinentes
        df = dataframes[0][SELECTED_COLUMNS].copy()

        # Étape 5 : Résolution des références
        dataset = df.copy()
        for col in ["freight_cost_usd", "weight_kilograms"]:
            df[col] = df[col].astype(str).apply(
                lambda x: resolve_id_reference(x, dataset, col)
            ).astype(float, errors='ignore')

        # Étape 6 : Nettoyage des données
        df = df.dropna(subset=["shipment_mode", "line_item_insurance_usd"])
        print(f"Après nettoyage, {len(df)} lignes restantes.")

        # Étape 7 : Création des tables de dimension
        df, countries, vendors, transport_modes, products = create_dimension_tables(df)

        # Étape 8 : Connexion à PostgreSQL
        pg_conn = setup_postgres_database()

        # Étape 9 : Création et ingestion des tables de dimension
        create_table(pg_conn, "countries", countries)
        insert_data(pg_conn, "countries", countries)

        create_table(pg_conn, "vendors", vendors)
        insert_data(pg_conn, "vendors", vendors)

        create_table(pg_conn, "transport_modes", transport_modes)
        insert_data(pg_conn, "transport_modes", transport_modes)

        create_table(pg_conn, "products", products)
        insert_data(pg_conn, "products", products)

        # Étape 10 : Création et ingestion de la table principale avec clés étrangères
        foreign_keys = [
            {"column": "country_id", "ref_table": "countries", "ref_column": "country_id"},
            {"column": "vendor_id", "ref_table": "vendors", "ref_column": "vendor_id"},
            {"column": "mode_id", "ref_table": "transport_modes", "ref_column": "mode_id"},
            {"column": "product_id", "ref_table": "products", "ref_column": "product_id"}
        ]
        create_table(pg_conn, "deliveries", df, foreign_keys)
        insert_data(pg_conn, "deliveries", df)

        # Étape 11 : Affichages
        display_head(df, "Aperçu de la table deliveries")
        display_head(countries, "Aperçu de la table countries")
        display_head(vendors, "Aperçu de la table vendors")
        display_head(transport_modes, "Aperçu de la table transport_modes")
        display_head(products, "Aperçu de la table products")
        display_summary_stats(df, "Statistiques des colonnes numériques")
        display_value_counts(transport_modes, "mode_name", "Répartition des modes de transport")
        display_value_counts(countries, "country_name", "Répartition des pays")

        # Fermer la connexion
        pg_conn.close()
        print("Connexion à la base de données fermée.")

        return df

    except Exception as e:
        print(f"Erreur dans le pipeline : {e}")
        raise

if __name__ == "__main__":
    main()
