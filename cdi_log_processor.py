import os
import pandas as pd
import re
from typing import List, Tuple

DEBUG = True


def main() -> None:
    base_path = r"Z:\Benchmarks\HDD Info\Crystal Disk Info"
    generate_data(base_path)


def generate_data(base_path: str) -> None:
    print("----- generate_data -----")

    output_path = os.path.join(base_path, "output_data.csv")
    input_file_names = [f for f in os.listdir(base_path) if (
            os.path.isfile(os.path.join(base_path, f)) and f.startswith("CrystalDiskInfo_") and f.endswith(".txt"))]
    input_file_names.sort()

    columns = [
        "make",
        "model",
        "firmware",
        "serial_number",
        "size_gb",
        "rotation_rate",
        "interface",
        "power_on_hours",
        "power_on_count",
        "health_status",
        "host_reads_GB",
        "host_writes_GB",
        "NAND_writes_GB",
        "info_date"
    ]

    df = pd.DataFrame(columns=columns)
    # Use a set for better performance when checking for duplicates
    serial_number_set = set()

    for input_file_name in input_file_names:
        input_file_path = os.path.join(base_path, input_file_name)
        encoding = "utf-8"
        with open(input_file_path, "r", encoding=encoding) as input_file:
            if DEBUG:
                print("Read from {}".format(input_file_path))

            lines: List[str] = list(input_file)

            date = ""
            new_row = {}

            for line in lines:
                line = line.strip()

                if line.startswith("Date : "):
                    date = line.replace("Date : ", "")

                if line == "----------------------------------------------------------------------------":
                    # Start signal
                    new_row = {"info_date": date}

                if line.startswith("Model : "):
                    model_text = line.replace("Model : ", "")
                    make, model = get_make_and_model(model_text)
                    new_row["make"] = make
                    new_row["model"] = model

                if line.startswith("Firmware : "):
                    new_row["firmware"] = line.replace("Firmware : ", "")

                if line.startswith("Serial Number : "):
                    new_row["serial_number"] = line.replace("Serial Number : ", "")

                if line.startswith("Disk Size : "):
                    size_match_object = re.search(r"\d+\.\d+ GB", line)
                    if size_match_object is not None:
                        size_value = size_match_object.group().replace(" GB", "")
                        new_row["size_gb"] = size_value

                if line.startswith("Rotation Rate : "):
                    rotation_rate_text = line.replace("Rotation Rate : ", "")
                    if " RPM" in rotation_rate_text:
                        new_row["rotation_rate"] = rotation_rate_text.replace(" RPM", "")
                    elif " (SSD)" in rotation_rate_text:
                        new_row["rotation_rate"] = "SSD"
                    else:
                        new_row["rotation_rate"] = rotation_rate_text

                if line.startswith("Interface : "):
                    new_row["interface"] = line.replace("Interface : ", "")

                if line.startswith("Power On Hours : "):
                    power_on_hours_match_object = re.search(r"\d+ hours", line)
                    if power_on_hours_match_object is not None:
                        power_on_hours_value = power_on_hours_match_object.group().replace(" hours", "")
                        new_row["power_on_hours"] = power_on_hours_value

                if line.startswith("Power On Count : "):
                    power_on_count_match_object = re.search(r"\d+ count", line)
                    if power_on_count_match_object is not None:
                        power_on_count_value = power_on_count_match_object.group().replace(" count", "")
                        new_row["power_on_count"] = power_on_count_value

                if line.startswith("Health Status : "):
                    health_status_text = line.replace("Health Status : ", "")
                    if " (100 %)" in health_status_text:
                        new_row["health_status"] = health_status_text.replace(" (100 %)", "")
                    else:
                        new_row["health_status"] = health_status_text
                
                if line.startswith("Host Reads : "):
                    host_reads_match_object = re.search(r"\d+ GB", line)
                    if host_reads_match_object is not None:
                        host_reads_value = host_reads_match_object.group().replace(" GB", "")
                        new_row["host_reads_GB"] = host_reads_value

                if line.startswith("Host Writes : "):
                    host_writes_match_object = re.search(r"\d+ GB", line)
                    if host_writes_match_object is not None:
                        host_writes_value = host_writes_match_object.group().replace(" GB", "")
                        new_row["host_writes_GB"] = host_writes_value
                
                if line.startswith("NAND Writes : "):
                    NAND_writes_match_object = re.search(r"\d+ GB", line)
                    if NAND_writes_match_object is not None:
                        NAND_writes_value = NAND_writes_match_object.group().replace(" GB", "")
                        new_row["NAND_writes_GB"] = NAND_writes_value

                if line == "-- S.M.A.R.T. --------------------------------------------------------------":
                    # End signal
                    # Remove duplicates
                    serial_number = new_row["serial_number"]
                    if serial_number in serial_number_set:
                        df = df[df["serial_number"] != serial_number]
                        if DEBUG:
                            print("Removed duplicate record for disk with serial number {}".format(serial_number))

                    serial_number_set.add(serial_number)
                    df = df.append(new_row, ignore_index=True)
                    if DEBUG:
                        print("Added a new row {}".format(new_row))

    # df.sort_values(by="date", inplace=True)
    df.to_csv(output_path, index=False)
    if DEBUG:
        print("Saved to {}".format(output_path))

    print("----- generate_data -----")
    print()

def get_make_and_model(model_text: str) -> Tuple[str, str]:
    if " " in model_text:
        return tuple(model_text.split(" ", 1))
    elif model_text.startswith("ST"):
        return "Seagate", model_text
    elif model_text.startswith("CT"):
        return "Crucial", model_text
    elif model_text.startswith("HDS"):
        return "Hitachi", model_text
    elif model_text.startswith("HFM"):
        return "SK Hynix", model_text
    elif model_text.startswith("WD"):
        return "WDC", model_text
    elif model_text.startswith("Micron"):
        return "Micron", model_text
    elif model_text.startswith("STM"):
        return "Seagate Maxtor", model_text
    elif model_text.startswith("F2C"):
        return "Fortinet OCZ", model_text
    else:
        return "", model_text


if __name__ == "__main__":
    main()
