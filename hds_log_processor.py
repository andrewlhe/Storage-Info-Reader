import os
import pandas as pd
import re
from typing import List, Tuple
from sys import platform

DEBUG = True


def main() -> None:

    base_path = r"Z:\Benchmarks\HDD Info\HD Sentinel"
    if platform == "darwin":
        base_path = "/Users/haoyuanxia/Downloads/Storage Media/HDS Logs"
    generate_data(base_path)


def generate_data(base_path: str) -> None:
    print("----- generate_data -----")

    output_path = os.path.join(base_path, "output_data.csv")
    input_file_names = [f for f in os.listdir(base_path) if (
            os.path.isfile(os.path.join(base_path, f)) and f.startswith("Disk report") and f.endswith(".txt"))]
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
        "info_date",
        "owner",
        "listed",
        "sold",
        "sold_date",
        "price"
    ]

    df = pd.DataFrame(columns=columns)
    # Use a set for better performance when checking for duplicates
    serial_number_set = set()

    for input_file_name in input_file_names:
        input_file_path = os.path.join(base_path, input_file_name)
        encoding = "cp1252"
        with open(input_file_path, "r", encoding=encoding) as input_file:
            if DEBUG:
                print("Read from {}".format(input_file_path))

            lines: List[str] = list(input_file)

            date = ""
            new_row = {}

            for line in lines:
                line = line.strip()

                if line.startswith("Current Date And Time"):
                    colon_index = line.index(": ")
                    date = line[colon_index + 2:]

                if line.startswith("-- Physical Disk Information"):
                    # Start signal
                    new_row = {"info_date": date}

                if line.startswith("Hard Disk Model ID"):
                    colon_index = line.index(": ")
                    model_text = line[colon_index + 2:]
                    make, model = get_make_and_model(model_text)
                    new_row["make"] = make
                    new_row["model"] = model

                if line.startswith("Firmware Revision"):
                    colon_index = line.index(": ")
                    new_row["firmware"] = line[colon_index + 2:]

                if line.startswith("Hard Disk Serial Number"):
                    colon_index = line.index(": ")
                    new_row["serial_number"] = line[colon_index + 2:]

                if line.startswith("Total Size"):
                    size_match_object = re.search(r"\d+ MB", line)
                    if size_match_object is not None:
                        size_value = size_match_object.group().replace(" MB", "")
                        new_row["size_gb"] = "{:.1f}".format(int(size_value) * 1024 * 1024 / 1000 / 1000 / 1000)

                if line.startswith("Rotational Speed") or (
                        line.startswith("Nominal Media Rotation Rate") and "rotation_rate" not in new_row):
                    # Use "Nominal Media Rotation Rate" if "Rotational Speed" is not available
                    colon_index = line.index(": ")
                    rotation_rate_text = line[colon_index + 2:]
                    if " RPM" in rotation_rate_text:
                        new_row["rotation_rate"] = rotation_rate_text.replace(" RPM", "")
                    elif " (SSD)" in rotation_rate_text:
                        new_row["rotation_rate"] = "SSD"
                    else:
                        new_row["rotation_rate"] = rotation_rate_text

                if line.startswith("Disk Interface"):
                    colon_index = line.index(": ")
                    new_row["interface"] = line[colon_index + 2:]

                if line.startswith("Power On Time"):
                    power_on_hours = 0
                    power_on_days_match_object = re.search(r"\d+ days", line)
                    power_on_hours_match_object = re.search(r"\d+ hours", line)
                    power_on_minutes_match_object = re.search(r"\d+ minutes", line)
                    if power_on_days_match_object is not None:
                        power_on_days_value = power_on_days_match_object.group().replace(" days", "")
                        power_on_hours += int(power_on_days_value) * 24
                    if power_on_hours_match_object is not None:
                        power_on_hours_value = power_on_hours_match_object.group().replace(" hours", "")
                        power_on_hours += int(power_on_hours_value)
                    if power_on_minutes_match_object is not None:
                        power_on_minutes_value = power_on_minutes_match_object.group().replace(" minutes", "")
                        power_on_hours += int(power_on_minutes_value) / 60
                    new_row["power_on_hours"] = "{:.0f}".format(power_on_hours)

                if line.startswith("Accumulated start-stop cycles"):
                    power_on_count_match_object = re.search(r"\d+$", line)
                    if power_on_count_match_object is not None:
                        power_on_count_value = power_on_count_match_object.group()
                        new_row["power_on_count"] = power_on_count_value

                if line.startswith("Health"):
                    colon_index = line.index(": ")
                    health_status_text = line[colon_index + 2:]

                    health_status_match_object = re.search(r"(?<=\()[\w\s]+(?=\))", health_status_text)
                    if health_status_match_object is not None:
                        health_status = health_status_match_object.group()
                    else:
                        health_status = "Unknown"

                    health_percentage_match_object = re.search(r"\d+(?= %)", health_status_text)
                    if health_percentage_match_object is not None:
                        health_status += " ({}%)".format(health_percentage_match_object.group())

                    new_row["health_status"] = health_status

                if line == "Transfer Rate Information":
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
        return tuple(model_text.split(" ", maxsplit=1))
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
    else:
        return "", model_text


if __name__ == "__main__":
    main()
