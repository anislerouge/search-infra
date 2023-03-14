import pandas as pd


def get_statut(statuts):
    if statuts:
        statuts_clean = [statut for statut in statuts if str(statut) != "nan"]
        if "ENGAGEE" in statuts_clean:
            return "valide"
    return "invalide"


def preprocess_agence_bio_data(data_dir):
    df_agence_bio = pd.read_csv(
        "https://object.files.data.gouv.fr/data-pipeline-open/"
        "prod/agence_bio/latest/agence_bio_certifications.csv",
        dtype=str,
    )

    df_agence_bio = df_agence_bio[["siret", "id_bio", "etat_certification"]]
    df_agence_bio = df_agence_bio[df_agence_bio["siret"].str.len() == 14]

    df_list_bio = (
        df_agence_bio.groupby(["siret"])["id_bio"]
        .apply(list)
        .reset_index(name="liste_id_bio")
    )

    df_list_statut = (
        df_agence_bio.groupby(["siret"])["etat_certification"]
        .apply(list)
        .reset_index(name="statut_bio")
    )
    df_list_bio = pd.merge(df_list_bio, df_list_statut, on="siret", how="left")
    df_list_bio["statut_bio"] = df_list_bio["statut_bio"].apply(lambda x: get_statut(x))
    df_list_bio["liste_id_bio"] = df_list_bio["liste_id_bio"].astype(str)
    df_list_bio["siren"] = df_list_bio["siret"].str[:9]
    del df_agence_bio

    return df_list_bio