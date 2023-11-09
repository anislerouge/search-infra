replace_table_siret_siege_query = """
        REPLACE INTO siretsiege
        (
            siren,
            siret,
            date_creation,
            tranche_effectif_salarie,
            annee_tranche_effectif_salarie,
            date_mise_a_jour,
            activite_principale_registre_metier,
            est_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2
        ) SELECT
            a.siren,
            a.siret,
            a.date_creation,
            a.tranche_effectif_salarie,
            a.annee_tranche_effectif_salarie,
            a.date_mise_a_jour,
            a.activite_principale_registre_metier,
            a.est_siege,
            a.numero_voie,
            a.type_voie,
            a.libelle_voie,
            a.code_postal,
            a.libelle_cedex,
            a.libelle_commune,
            a.commune,
            a.complement_adresse,
            a.complement_adresse_2,
            a.numero_voie_2,
            a.indice_repetition_2,
            a.type_voie_2,
            a.libelle_voie_2,
            a.commune_2,
            a.libelle_commune_2,
            a.cedex_2,
            a.libelle_cedex_2,
            a.cedex,
            a.date_debut_activite,
            a.distribution_speciale,
            a.distribution_speciale_2,
            a.etat_administratif_etablissement,
            a.enseigne_1,
            a.enseigne_2,
            a.enseigne_3,
            a.activite_principale,
            a.indice_repetition,
            a.nom_commercial,
            a.libelle_commune_etranger,
            a.code_pays_etranger,
            a.libelle_pays_etranger,
            a.libelle_commune_etranger_2,
            a.code_pays_etranger_2,
            a.libelle_pays_etranger_2
        FROM flux_siret a LEFT JOIN siretsiege b
        ON a.siret = b.siret
        WHERE a.est_siege = 'true'
    """
