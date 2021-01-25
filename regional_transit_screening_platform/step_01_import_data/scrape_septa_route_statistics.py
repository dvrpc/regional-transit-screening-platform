# """
# `scrape_septa_route_statistics.py`
# ----------------------------------

# This script exists to scrape data from SEPTA's Route Statistics report.
# As of 9/25/2020, the latest version available is the 2019 report as a PDF.

# Dependencies:
# -------------

# pypdf2 - extracts quickly but destroys any semblance of formatting
# pdfminer - much slower, but is more respectful of the original format

# """
# import pandas as pd
# from tqdm import tqdm
# import PyPDF2
# from pdfminer.high_level import extract_text
# from pathlib import Path

# from regional_transit_screening_platform import db, file_root


# # We need this to be a string instead of pathlib.Path
# septa_file = file_root / "other data" / "2019 Route Statistics.pdf"
# septa_file = str(septa_file)


# def classify_pages(
#         filepath: str = septa_file,
#         start_page: int = 9,
#         end_page: int = 324):
#     """
#         Use pypdf2 to classify each page as 'data' or 'other'

#         Returns:
#             - dictionary keyed on classification
#             - values are lists of page numbers

#         Note: pypdf2 zero-indexes the pages, so they'll
#         be one off from the actual PDF page number
#     """

#     page_number_classification = {"data": [], "other": []}
#     page_range = range(start_page, end_page)

#     print("Classifying pages ")

#     with open(septa_file, 'rb') as open_pdf:
#         pdf = PyPDF2.PdfFileReader(open_pdf)

#         for page_number in tqdm(page_range, total=len(page_range)):
#             page = pdf.getPage(page_number)

#             text = page.extractText()

#             # Data pages seem to all have "®SEPTA"
#             if "®SEP" in text:
#                 status = "data"
#             else:
#                 status = "other"

#             page_number_classification[status].append(page_number)

#     return page_number_classification


# def extract_data(filepath: Path, page_numbers: list):
#     """
#         Use pdfminer to extract data from the pages in the page_numbers list

#         Returns:
#             - dictionary keyed on PDF page number
#             - values are the page text as a list, split by '\n'
#     """

#     data = {}
#     for page_number in tqdm(page_numbers, total=len(page_numbers)):
#         text = extract_text(septa_file, page_numbers=[page_number])
#         data[page_number + 1] = text.split("\n")

#     return data


# def find_stats_in_list(data: list):

#     slice_start = "OPERATING STATISTICS"

#     if slice_start not in data:
#         # Try adding a trailing space. Some have these
#         slice_start += " "
#         if slice_start not in data:
#             return {}

#     # The data we want is between "OPERATING STATISTICS" and "CHARACTERISTICS"
#     start_idx = data.index(slice_start)
#     end_idx = data.index("CHARACTERISTICS")

#     # Make a subset that ONLY includes the data to extract
#     subset = data[start_idx: end_idx - 1]

#     # Get the index value of the empty lines. There should be 2
#     idx_list = [i for i, x in enumerate(subset) if x == ""]

#     if len(idx_list) < 2:
#         return dict(subset)
#     else:
#         labels = subset[idx_list[0] + 1: idx_list[1]]
#         values = subset[idx_list[1] + 1:]

#         return dict(zip(labels, values))


# def find_route_name(data: list):

#     if "SERVICE LEVELS" not in data:
#         return "QAQC - different format"

#     end_idx = data.index("SERVICE LEVELS")
#     header = data[:end_idx]

#     possible_route_names = []

#     for item in header:
#         if 0 < len(item) <= 5 or item == "Direct":
#             possible_route_names.append(item)

#     if len(possible_route_names) == 0:
#         return "QAQC - no route name options"
#     elif len(possible_route_names) > 1:
#         return "QAQC - multiple possible route names: " + ", ".join(possible_route_names)

#     else:
#         return possible_route_names[0]


# def scrape_septa_report(filepath: str = septa_file):

#     all_stats = []

#     # Figure out which pages have data we want quickly
#     page_classification = classify_pages(septa_file)
#     data_pages = page_classification["data"]

#     # Scrape the pages with data using a slower but better method
#     data = extract_data(septa_file, data_pages)

#     # For each scraped page, try to extract stats and route name
#     for page_number in data:
#         full_dataset = data[page_number]

#         route_name = find_route_name(full_dataset)
#         stats = find_stats_in_list(full_dataset)

#         stats["route_name"] = route_name
#         stats["page_number"] = page_number

#         all_stats.append(stats)

#     # Combine results from each page into a single dataframe
#     df = pd.DataFrame(all_stats)

#     # We end up with a few extra columns from some of the more
#     # messy pages, so let's be specific about which columns to keep
#     columns = [
#         'ONE WAY ROUTE MILES (AVG.)',
#         'DAILY AVERAGE (WK) RIDERSHIP',
#         'VEHICLE HOURS (ANNUAL)',
#         'VEHICLE MILES (ANNUAL)',
#         'PEAK VEHICLES',
#         'FULLY ALLOCATED EXPENSES',
#         'PASSENGER REVENUE',
#         'OPERATING RATIO',
#         'ON TIME % (SEASON)',
#         'route_name',
#         'page_number'
#     ]

#     df = df[columns]

#     # Save the dataframe to SQL
#     db.import_dataframe(df, "septa_report_scrape_2019", if_exists="replace")


# if __name__ == "__main__":
#     scrape_septa_report()
