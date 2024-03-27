import re
import pandas as pd


# List of patterns to search
compound_ID_list_patterns = [r"\w\w?B2-\d+", r"\w\w?B-\d+", r"\w\w?C2-\d+", r"\w\w?C-\d+",
                             r"\w\w?N2-\d+", r"\w\w?N-\d+", r"\w\w?O2-\d+", r"\w\w?O-\d+",
                             r"\w\w?P2-\d+", r"\w\w?P-\d+", r"\w\w?S2-\d+", r"\w\w?S-\d+"]

# List of molecule groups
molecule_groups = ["B2", "B", "C2", "C", "N2", "N", "O2", "O", "P2", "P", "S2", "S"]


for compound_ID, molecule_group in zip(compound_ID_list_patterns, molecule_groups):

    # Opening the text files containing the material data
    with open(f"./data/raw/M{molecule_group}-energy.dat", "r") as file:
        text_to_parse = file.read()

    # Searching for the material name
    compound_ID_regex = re.compile(compound_ID)
    compound_ID_matches = compound_ID_regex.finditer(text_to_parse)

    # Getting all the material names
    compound_ID_list = []
    for ID in compound_ID_matches:
        compound_ID_list.append(ID.group())

    # Pattern for atom numbers
    atom_numbers_pattern = r"[^_](\d\d?) (\d\d?)"

    # Searching for the atom numbers
    atom_numbers_regex = re.compile(atom_numbers_pattern)
    atom_numbers_matches = atom_numbers_regex.finditer(text_to_parse)

    # Getting the total number of atoms for each material name
    total_atom_number = []
    for val in atom_numbers_matches:
        total_atom_number.append(float(val.group(1)) + float(val.group(2)))

    # Pattern for the total energy
    energy_pattern = r"F= (-\.\d+E[+-]\d\d)"

    # Searching for the total energies
    energy_pattern_regex = re.compile(energy_pattern)
    energy_matches = energy_pattern_regex.finditer(text_to_parse)

    # Getting the total energy for each material name
    energy_list = []
    for energy in energy_matches:
        energy_list.append(float(energy.group(1)))

    # Calculating the per-atom total energy for each material name
    energy_per_atom = [0 for i in range(len(energy_list))]
    for i in range(len(energy_list)):
        energy_per_atom[i] = energy_list[i] / total_atom_number[i]

    # Creating the dataframe
    data = zip(compound_ID_list, energy_per_atom)
    df = pd.DataFrame(data, columns=["Compound_ID", "Energy_per_atom"])

    # Storing the dataframe in a CSV file
    df.to_csv(f"./data/raw/M{molecule_group}-per-atom.dat")
