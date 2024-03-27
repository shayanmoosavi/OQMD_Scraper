from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from time import sleep


# INITIALIZATIONS
# ===============================================================================================

# The url of the website being scraped
domain_url = "https://oqmd.org"
# List of molecule groups we are querying
molecule_groups = ["B2", "B", "C2", "C", "N2", "N", "O2", "O", "P2", "P", "S2", "S"]

# User agent header to tell the website information about your system and browser.
# For better results, search "my user agent" and replace it.
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}


# WEB SCRAPING OUR DESIRED DATA FROM OQMD DATABASE
# ===============================================================================================

# List of dataframes to be concatenated
frames = []
# Iterating over the molecule groups to get all the data
for molecule_group in molecule_groups:

    print(f"Reading M{molecule_group}-per-atom.dat...\n")
    # Reading the per-atom data
    per_atom_df = pd.read_csv(f"./data/raw/M{molecule_group}-per-atom.dat", index_col=0)
    Compound_ID_List = per_atom_df["Compound_ID"]

    # Splitting the material names from the ID after their names and storing them in a list
    material_names = [compound_id.split('-')[0] for compound_id in Compound_ID_List.values]

    # Iterating over each material name
    for material_name in material_names:
        # The directory of the corresponding link for this material in OQMD website
        composition_link = f"/materials/composition/{material_name}"

        print(f"Reading page content for {material_name}...\n")

        # Retrying until the page is opened and its HTML content is retrieved
        composition_page_html_content = ''
        while composition_page_html_content == '':
            try:
                # Putting 5-second delays between requests in order to avoid
                # sending too many requests, resulting in server ERROR 429.
                sleep(5)
                composition_page_html_content = requests.get(domain_url + composition_link, headers=header).text
            except requests.exceptions.ConnectionError:
                # Retrying after 15 seconds if we get a connection error
                sleep(15)
                continue

        # Retrieving the HTML content of the page
        soup = BeautifulSoup(composition_page_html_content, "lxml")
        # Finding the table in the page
        table = soup.find("table")

        try:
            # Getting all the table rows
            table_rows_html = table.find_all("tr", class_="clickableRow")
            # Getting the link for different material IDs for the specified material name
            material_links = [row.get("href") for row in table_rows_html]

            # Regex pattern for retrieving the material IDs from the link
            ID_pattern = re.compile(r"/materials/entry/(\d+)")
            ID_matches = [ID_pattern.finditer(link) for link in material_links]

            # Finding and storing the IDs in a list
            ID_list = []
            for ID_match in ID_matches:
                for ID in ID_match:
                    ID_list.append(int(ID.group(1)))

            i = 0  # Counter
            # List of material dataframes to be concatenated
            material_name_frames = []
            # Iterating over each ID for the specified material
            for material_link in material_links:

                url = domain_url + material_link
                print(f"Reading page content for {material_link}...\n")

                # Retrying until the page is opened and its HTML content is retrieved
                material_id_page_html_content = ''
                while material_id_page_html_content == '':
                    try:
                        # Putting 5-second delays between requests in order to avoid
                        # sending too many requests, resulting in server ERROR 429.
                        sleep(5)
                        material_id_page_html_content = requests.get(url, headers=header).text
                    except requests.exceptions.ConnectionError:
                        # Retrying after 10 seconds if we get a connection error
                        sleep(10)
                        continue

                soup = BeautifulSoup(material_id_page_html_content, "lxml")
                table = soup.find("table")

                print("Reading table contents...\n")
                try:
                    if i == 0:
                        # Getting the column names from the table in the page
                        table_head_html = table.find_all("th")
                        column_names = [column_name.text for column_name in table_head_html]
                    # Getting table rows
                    table_rows_html = table.find_all("tr", class_="clickableRow")
                    # Creating an empty dataframe for each material ID
                    Energy_Calculations_df = pd.DataFrame(columns=column_names)

                    # Extracting all the data from the table
                    for row in table_rows_html:
                        row_data_html = row.find_all("td")
                        row_data = [data.text for data in row_data_html]
                        # Inserting the row data into the empty dataframe we created
                        Energy_Calculations_df.loc[len(Energy_Calculations_df)] = row_data
                    # Setting the "Configuration" column as the index column
                    Energy_Calculations_df.set_index("Configuration", inplace=True)
                    # Appending all the material ID dataframes to create a dataframe for each material name
                    material_name_frames.append(Energy_Calculations_df)
                    print("Finished reading data...")
                    i += 1

                # Skipping the ID that does not have a data
                except AttributeError:
                    print(f"No data exists for {material_link}. skipping...")
                    continue

            # Creating a dataframe for each material name
            material_name_df = pd.concat(material_name_frames, keys=pd.MultiIndex.from_product([
                [material_name], ID_list], names=["Material", "ID"]))
            print(material_name_df)
            # Appending the material name dataframe to the main dataframe
            frames.append(material_name_df)

        # Skipping the material that does not have any data
        except AttributeError:
            print(f"No data exists for {material_name}. skipping...")
            continue
    # Creating the final dataframe for all the materials of a specific molecule group
    final_df = pd.concat(frames)
    # Storing the dataframe in a CSV file
    final_df.to_csv(f"./data/raw/M{molecule_group}-OQMD-Data.dat")
