from helpers.s3_helpers import s3_client


def rename_old_rne_folders(**kwargs):
    s3_client.rename_folder("rne/flux/data-2023", "rne/flux/data")
