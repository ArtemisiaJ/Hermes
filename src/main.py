# Importing generic external libraries, which are widely used in Python coding.
import os
import shutil
from datetime import datetime

# 'Pandas' can be used to generate a 'dataframe' which can then save data as csv.
import pandas as pd

# Thread is used to enable multithreading.
from threading import Thread

# TK is a basic GUI library, but is used in this program only as a dialog box.
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
    # Opens a file selector dialog box to select the location for data to be transferred FROM.
    tkinter.Tk().withdraw()
    path = filedialog.askdirectory()

    # Parses the file structure of the drive selected above, storing as attributes in accordance with CasePath class.
    cases_and_paths = get_cases_and_paths(path)

    # This dictates the 'destination drive', and should always be "U" for the DFU ISILON.
    isilon_drive = "U"

    # Primary method for transferring & hashing data using variables declared above.
    transfer_data_to_isilon(drive=isilon_drive, cases_and_paths=cases_and_paths, path=path)


def get_cases_and_paths(path):
    cases_and_paths = []

    # Parses through the folder structure of 'path', returning the "case id" which should be present within each CCL
    # 'parent_ref'.
    for parent_ref in next(os.walk(path))[1]:
        if parent_ref[:3] == "KNT":
            client_path = os.path.join(path, parent_ref)
            for item in next(os.walk(client_path))[1]:

                # Tries to convert the first 5 chars of 'item' which is in essence the case_id. If ValueError, then
                # the folder will be marked as 'invalid' and requiring manual attention. CCL should be contacted and
                # tasked to provide data in this consistent format.
                try:
                    int(item[:5])
                    case_and_path = CasePaths(case_id=item[:5], original_path=client_path, valid=True)
                except ValueError:
                    case_and_path = CasePaths(case_id=item, original_path=client_path, valid=False)
                cases_and_paths.append(case_and_path)
    return cases_and_paths


def hash_dir(input_path, output_path):
    # Uses 7-Zip (console version) to calculate the SHA256 hash of the CRC of the directory 'input_path', and saves
    # the log to \.temp.
    os.system(f"7za.exe h -scrcSHA256 {input_path} > {output_path}")

    #   Use 7za h -scrcSHA256 "{input_path}" > "{output_path}"
    #
    #   This will calculate the SHA256/CRC of the input_path and direct the progress log to the output_path. The output
    #   path can then be read by the script to obtain the SHA256 for data/data and names and then compare to the hashes
    #   of the copied files. It would be good to find a more 'Pythonic' way to calculate and store hashes as variables
    #   as this will remove the need for storing log files and using external programs that cannot be multithreaded as
    #   easily as in the main Python script.


def copy_dir(original_filepath, new_filepath):
    # 'Copytree' is a method of copying entire directories whilst preserving metadata.
    shutil.copytree(original_filepath, new_filepath)


def transfer_data_to_isilon(drive, cases_and_paths, path):
    failed_transfers = []
    invalid_references = []
    successful_transfers = []

    for case in cases_and_paths:
        case_id_no_year = str(case.case_id).replace("-23", "")
        case_path = f"{drive}:\\{case_id_no_year}"
        extraction_path_new = f"{case_path}\\Extracted Data"
        exhibits_path_new = f"{case_path}\\Exhibits"

        extraction_path_new_zip = f'"{case_path}\\Extracted Data"'

        # Prints log of transfer commencing for case, and stating where the files will be sent to.
        print(f"\nCommenced processing job {case_id_no_year}. Files will be sent to {extraction_path_new_zip}")

        if case.valid:
            try:
                # Generates the case directories on the DFU ISILON, and captures the date/time the transfer commenced.
                os.makedirs(exhibits_path_new)
                started_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

                try:
                    # Declaring the original path, and paths for temporary files.
                    original_filepath = f"{case.original_path}"
                    original_hash_log = f".temp\\{case_id_no_year}-1.txt"
                    new_hash_log = f".temp\\{case_id_no_year}-2.txt"

                    # Declaring the different 'threads' which are used to speed the processing of data.
                    copy_thread = Thread(target=copy_dir, args=(original_filepath, extraction_path_new))
                    original_hash_thread = Thread(target=hash_dir, args=(original_filepath, original_hash_log))
                    new_hash_thread = Thread(target=hash_dir, args=(extraction_path_new_zip, new_hash_log))

                    # 1 - Start hashing original directory.
                    original_hash_thread.start()

                    # 2 - Start copying original directory to DFU ISILON.
                    copy_thread.start()

                    # 3 - Wait for step '2' to finish.
                    copy_thread.join()

                    # 4 - Start hashing new directory.
                    new_hash_thread.start()

                    # 5 - Wait for steps '1' and '4' to finish before proceeding.
                    original_hash_thread.join()
                    new_hash_thread.join()

                    # Parses the log files stored in '\.temp' for the SHA256 hashes calculated by 7-Zip.
                    with open(original_hash_log) as file:
                        while line := file.readline():
                            if "SHA256 for data:" in line:
                                original_hash = line.replace("SHA256 for data:", "").strip()
                                break
                    with open(new_hash_log) as file:
                        while line := file.readline():
                            if "SHA256 for data:" in line:
                                new_hash = line.replace("SHA256 for data:", "").strip()
                                break

                    # Removes log files.
                    for file in [original_hash_log, new_hash_log]:
                        os.remove(file)

                    # Calculates date/time process finished.
                    finished_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

                    # Saves details of process to csv.
                    dataframe = pd.DataFrame({'Original Filepath': [original_filepath],
                                              'New Filepath': [extraction_path_new],
                                              'Original SHA256 for data': [original_hash],
                                              'New SHA256 for data': [new_hash],
                                              'Date/Time Started (UTC)': [started_timestamp],
                                              'Date/Time Finished (UTC)': [finished_timestamp]}, )
                    file_timestamp = datetime.utcnow().strftime("%Y-%m-%d")
                    hash_location = f"{extraction_path_new}\\{case_id_no_year}-hash list-{file_timestamp}.csv"
                    dataframe.to_csv(hash_location, index=False)

                    # Checks that the hashes match, if not then marks a failed transfer.
                    if not original_hash == new_hash:
                        failed_transfer = HashVerification(case_id=case.case_id, original_hash=original_hash,
                                                           new_hash=new_hash)
                        failed_transfers.append(failed_transfer)

                        print(f"Hashes do not match!\n"
                              f"Original Hash: {original_hash}\n"
                              f"New Hash: {new_hash}")

                    print(f"Transfer of {case_id_no_year} completed with no errors.")
                    successful_transfer = HashVerification(case_id=case.case_id, original_hash=original_hash,
                                                           new_hash=new_hash)
                    successful_transfers.append(successful_transfer)

                # If the directory already exists, then the transfer is not attempted as it will crash the program.
                except FileExistsError:
                    case.valid = False

                    print(f"Exhibit for {case_id_no_year} has already been transferred!")

            # This handles multiple exhibits for the same case.
            except FileExistsError:
                print("Case Directory already exists! Proceeding...")

        # Checks for if case.valid if false, as this will indicate that the process for that case was not successful.
        if case.valid is False:
            invalid_case = InvalidCases(invalid_folder_name=case.case_id, original_path=case.original_path)
            invalid_references.append(invalid_case)

            print(f"Transfer of {case_id_no_year} unsuccessful.")

    # Generates log file with details of cases which were not processed successfully, and require manual attention.
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H-%M")
    log_file = f"{path}\\Hermes Log {timestamp}.txt"
    with open(file=log_file, mode="w") as f:

        # If no errors encountered, just lists successful transfers.
        if not failed_transfers and not invalid_references:
            f.write("|---------------------------------------|\n"
                    "| All transfers completed successfully! |\n"
                    "|---------------------------------------|\n"
                    "\n| Successful Transfers:\n")
            for c in successful_transfers:
                f.write(f"| - {c.case_id} ({c.new_hash})\n")

        # If errors encountered.
        else:
            f.write("|-----------------------------------------------------------------------------|\n"
                    "| Some transfers have failed to verify. Please see below for further details. |\n"
                    "|-----------------------------------------------------------------------------|\n")

            # If at least 1 case failed to verify after transferring due to hashes not matching.
            if failed_transfers is not False:
                for obj in failed_transfers:
                    f.write(f"\n"
                            f"| {obj.case_id} transfer failed! Hashes do not match.\n"
                            f"| - Original Hash: {obj.original_hash}\n"
                            f"| - New Hash: {obj.new_hash}\n")

            # If at least 1 case failed to transfer due to invalid folder name.
            if invalid_references is not False:
                for obj in invalid_references:
                    f.write(f"\n"
                            f"| Transfer failed for case due to invalid folder name. Manual attention is required.\n"
                            f"| - Invalid Folder Name: {obj.invalid_folder_name}\n"
                            f"| - Original File Path: {obj.original_path}\n")

            # Lists successful transfers.
            f.write("\n"
                    "| Successful Transfers:\n")
            for c in successful_transfers:
                f.write(f"| - {c.case_id} ({c.new_hash})\n")
    os.startfile(log_file)


if __name__ == '__main__':
    main()
