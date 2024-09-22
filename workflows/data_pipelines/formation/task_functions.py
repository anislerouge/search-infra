import pandas as pd
import logging
import requests

from helpers.s3_helpers import s3_client
from helpers.settings import Settings
from helpers.tchap import send_message


def preprocess_organisme_formation_data(ti):
    # get dataset directly from dge website
    r = requests.get(Settings.URL_ORGANISME_FORMATION)
    with open(Settings.FORMATION_TMP_FOLDER + "qualiopi-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_organisme_formation = pd.read_csv(
        Settings.FORMATION_TMP_FOLDER + "qualiopi-download.csv", dtype=str, sep=";"
    )

    # Renommage des colonnes
    # Numéro Déclaration Activité;Numéro Déclaration Activité Précédent;denomination;Code SIREN;Siret Etablissement Déclarant;Adresse Organisme de Formation;Code Postal Organisme de Formation;Ville Organisme de Formation;Code Région Organisme de Formation;geocodageban;Actions de formations;Bilans de compétences;VAE;Actions de formations par apprentissage;Dénomination Organisme Etranger Représenté;Adresse Organisme Etranger Représenté;Code Postal Organisme Etranger Représenté;Ville Organisme Etranger Représenté;Pays Organisme Etranger Représenté;Date dernière déclaration;Début d'exercice;Fin d'exercice;Code Spécialité 1;Libellé Spécialité 1;Code Spécialité 2;Libellé Spécialité 2;Code Spécialité 3;Libellé Spécialité 3;Nombre de stagiaires;Nombre de stagiaires confiés par un autre Organisme de formation;Effectifs de formateurs;Code Commune;Nom Officiel Commune / Arrondissement Municipal;Code Officiel EPCI;Nom Officiel EPCI;Code Officiel Département;Nom Officiel Département;Nom Officiel Région;Code Officiel Région;Toutes spécialités de l'organisme de formation;Organisme Formation Géocodé;Certifications;random_id


    df_organisme_formation = df_organisme_formation.rename(
        columns={
            "Numéro Déclaration Activité": "id_nda",
            "Numéro Déclaration Activité Précédent": "id_nda_precedent",
            "denomination": "denomination",
            "Code SIREN": "siren",
            "Siret Etablissement Déclarant": "siret",
            "adresse": "adresse",
            "code_postal": "code_postal",
            "ville": "ville",
            "Code Région Organisme de Formation": "code_region",
            "geocodageban": "geocodageban",
            "Actions de formations": "cert_adf",
            "Bilans de compétences": "cert_bdc",
            "VAE": "cert_vae",
            "Actions de formations par apprentissage": "cert_app",
            "Dénomination Organisme Etranger Représenté": "denomination_organisme_etranger",
            "Adresse Organisme Etranger Représenté": "adresse_organisme_etranger",
            "Code Postal Organisme Etranger Représenté": "code_postal_organisme_etranger",
            "Ville Organisme Etranger Représenté": "ville_organisme_etranger",
            "Pays Organisme Etranger Représenté": "pays_organisme_etranger",
            "Date dernière déclaration": "date_derniere_declaration",
            "Début d'exercice": "debut_exercice",
            "Fin d'exercice": "fin_exercice",
            "Code Spécialité 1": "code_specialite_1",
            "Libellé Spécialité 1": "libelle_specialite_1",
            "Code Spécialité 2": "code_specialite_2",
            "Libellé Spécialité 2": "libelle_specialite_2",
            "Code Spécialité 3": "code_specialite_3",
            "Libellé Spécialité 3": "libelle_specialite_3",
            "Nombre de stagiaires": "nombre_stagiaires",
            "Nombre de stagiaires confiés par un autre Organisme de formation": "nombre_stagiaires_confies",
            "Effectifs de formateurs": "effectifs_formateurs",
            "Code Commune": "code_commune",
            "Nom Officiel Commune / Arrondissement Municipal": "nom_commune",
            "Code Officiel EPCI": "code_epci",
            "Nom Officiel EPCI": "nom_epci",
            "Code Officiel Département": "code_departement",
            "Nom Officiel Département": "nom_departement",
            "Nom Officiel Région": "nom_region",
            "Code Officiel Région": "code_region_officiel",
            "Toutes spécialités de l'organisme de formation": "toutes_specialites",
            "Organisme Formation Géocodé": "organisme_formation_geocode",
            "Certifications": "certifications",
        }
    )

    # Sauvegarde de la version 2 après renommage
    df_organisme_formation.to_csv(
        Settings.FORMATION_TMP_FOLDER + "qualiopi-download-v2.csv", index=False, sep=";"
    )

    # Grouper les données par SIRET et lister les ID_NDA et les certifications
    df_grouped = df_organisme_formation.groupby("siret").agg(
        id_nda=("id_nda", lambda x: ",".join(map(str, x))),
        certifications=("certifications", lambda x: ",".join(map(str, x)))
    ).reset_index()

    # Sauvegarder le fichier final
    df_grouped.to_csv(
        f"{Settings.FORMATION_TMP_FOLDER}formation.csv", index=False
    )

    # Stocker le nombre unique de SIRET dans XCom
    ti.xcom_push(
        key="nb_siret_formation",
        value=str(df_grouped["siret"].nunique()),
    )

    # Nettoyer les données inutiles
    del df_organisme_formation
    del df_grouped


def send_file_to_minio():
    s3_client.send_files(
        list_files=[
            {
                "source_path": Settings.FORMATION_TMP_FOLDER,
                "source_name": "formation.csv",
                "dest_path": "formation/new/",
                "dest_name": "formation.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = s3_client.compare_files(
        file_path_1="formation/new/",
        file_name_2="formation.csv",
        file_path_2="formation/latest/",
        file_name_1="formation.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    s3_client.send_files(
        list_files=[
            {
                "source_path": Settings.FORMATION_TMP_FOLDER,
                "source_name": "formation.csv",
                "dest_path": "formation/latest/",
                "dest_name": "formation.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(
        key="nb_siren_formation", task_ids="preprocess_organisme_formation_data"
    )
    send_message(
        f"\U0001F7E2 Données Organisme formation mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
