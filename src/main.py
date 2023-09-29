import os
import shutil
import pandas as pd
from datetime import datetime
from threading import Thread
from checksumdir import dirhash
import tkinter
from tkinter import filedialog


class HashVerification:
    def __init__(self, case_id, original_hash, new_hash):
        self.case_id = case_id
        self.original_hash = original_hash
        self.new_hash = new_hash


class CasePaths:
    def __init__(self, case_id, original_path, valid):
        self.case_id = case_id
        self.original_path = original_path
        self.valid = valid


class InvalidCases:
    def __init__(self, invalid_folder_name, original_path):
        self.invalid_folder_name = invalid_folder_name
        self.original_path = original_path


def main():
    tkinter.Tk().withdraw()
    path = filedialog.askdirectory()
    cases_and_paths = get_cases_and_paths(path)
    isilon_drive = "U"
    transfer_data_to_isilon(drive=isilon_drive, cases_and_paths=cases_and_paths, path=path)


def get_cases_and_paths(path):
    cases_and_paths = []
    for ccl_ref in next(os.walk(path))[1]:
        client_path = os.path.join(path, ccl_ref)
        for item in next(os.walk(client_path))[1]:
            try:
                int(item[:5])
                case_and_path = CasePaths(case_id=item[:5], original_path=client_path, valid=True)
            except ValueError:
                case_and_path = CasePaths(case_id=item, original_path=client_path, valid=False)
            cases_and_paths.append(case_and_path)
    return cases_and_paths


def hash_dir(path):
    sha256 = dirhash(path, "sha256")
    return sha256


def copy_dir(original_filepath, new_filepath):
    shutil.copytree(original_filepath, new_filepath)


def transfer_data_to_isilon(drive, cases_and_paths, path):
    failed_transfers = []
    invalid_references = []

    for case in cases_and_paths:
        case_id_no_year = str(case.case_id).replace("-23", "")
        case_path = f"{drive}:\\{case_id_no_year}"
        extraction_path_new = f"{case_path}\\Extracted Data"
        exhibits_path_new = f"{case_path}\\Exhibits"

        if case.valid:
            try:
                os.makedirs(case_path)
                os.makedirs(extraction_path_new)
                os.makedirs(exhibits_path_new)
                started_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

                original_filepath = f"{case.original_path}"
                new_filepath = f"{extraction_path_new}\\{case_id_no_year}"

                copy_thread = Thread(
                    target=copy_dir, args=(original_filepath, new_filepath)
                )

                copy_thread.start()
                original_hash = hash_dir(original_filepath)
                copy_thread.join()

                new_hash = hash_dir(new_filepath)

                finished_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

                dataframe = pd.DataFrame({'Original Filepath': [original_filepath],
                                          'New Filepath': [new_filepath],
                                          'Original SHA256': [original_hash],
                                          'New SHA256': [new_hash],
                                          'Date/Time Started (UTC)': [started_timestamp],
                                          'Date/Time Finished (UTC)': [finished_timestamp]}, )

                file_timestamp = datetime.utcnow().strftime("%Y-%m-%d")

                hash_location = f"{new_filepath}\\{case_id_no_year}-hash list-{file_timestamp}.csv"

                dataframe.to_csv(hash_location, index=False)

                if not original_hash == new_hash:
                    failed_transfer = HashVerification(case_id=case.case_id, original_hash=original_hash,
                                                       new_hash=new_hash)
                    failed_transfers.append(failed_transfer)

            except FileExistsError:
                case.valid = False

        if case.valid is False:
            invalid_case = InvalidCases(invalid_folder_name=case.case_id, original_path=case.original_path)
            invalid_references.append(invalid_case)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H-%M")

    log_file = f"{path}\\CCL Automate Log {timestamp}.txt"

    with open(file=log_file, mode="w") as f:

        if not failed_transfers and not invalid_references:
            f.write("|---------------------------------------|\n"
                    "| All transfers completed successfully! |\n"
                    "|---------------------------------------|\n")

        else:
            f.write("|-----------------------------------------------------------------------------|\n"
                    "| Some transfers have failed to verify. Please see below for further details. |\n"
                    "|-----------------------------------------------------------------------------|\n")

            if failed_transfers is not False:
                for obj in failed_transfers:
                    f.write(f"{obj.case_id} transfer failed! Hashes do not match.\n"
                            f"Original Hash: {obj.original_hash}\n"
                            f"New Hash: {obj.new_hash}\n")

            if invalid_references is not False:
                for obj in invalid_references:
                    f.write(f"\n"
                            f"| Transfer failed for case due to invalid folder name. Manual attention is required.\n"
                            f"| - Invalid Folder Name: {obj.invalid_folder_name}\n"
                            f"| - Original File Path: {obj.original_path}\n")

    os.startfile(log_file)


if __name__ == '__main__':
    main()
