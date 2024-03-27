import pandas as pd

# List of molecule groups we are querying
molecule_groups = ["B2", "B", "C2", "C", "N2", "N", "O2", "O", "P2", "P", "S2", "S"]

for molecule_group in molecule_groups:

    # Reading the per-atom data
    per_atom_df = pd.read_csv(f"./data/raw/M{molecule_group}-per-atom.dat", index_col=0)
    Compound_ID_List = per_atom_df["Compound_ID"]

    # Reading the OQMD data
    oqmd_df = pd.read_csv(f"./data/raw/M{molecule_group}-OQMD-Data.dat", index_col=("Material", "ID"))

    # Splitting the material names from the ID after their names and storing them in a list
    material_names = [compound_id.split('-')[0] for compound_id in Compound_ID_List.values]
    our_material_id = [compound_id.split('-')[1] for compound_id in Compound_ID_List.values]

    frames = []  # List of DataFrames to concatenate
    for index, material_name in enumerate(material_names):

        # Creating a DataFrame for each material name
        try:
            # Changing the name of material NaN because of avoiding getting confused with NaN values
            if material_name == "NaN":
                oqmd_material_df = oqmd_df.loc[("_NaN", slice(None))]
                per_atom_material_df = per_atom_df.iloc[index]
            else:
                oqmd_material_df = oqmd_df.loc[(material_name, slice(None))]
                per_atom_material_df = per_atom_df.iloc[index]

        # Skipping materials that does not have any data
        except KeyError as err:
            continue

        # Only selecting materials with "relaxation" or "standard" configuration
        relaxation_configuration_filter = oqmd_material_df["Configuration"] == "relaxation"
        standard_configuration_filter = oqmd_material_df["Configuration"] == "standard"
        # Filtering materials that are as stable or more stable than OQMD materials
        energy_threshold_filter = oqmd_material_df["Total energy [eV/atom]"] >= per_atom_material_df["Energy_per_atom"]

        # Creating a MultiIndex for the DataFrame
        stable_idx = pd.MultiIndex.from_product([[Compound_ID_List[index]],
                                                 oqmd_material_df.index.values],
                                                names=["Our_Material_ID", "OQMD_ID"])

        # Creating a DataFrame from the stable materials and with both energies to compare
        stable_material_df = pd.DataFrame(index=stable_idx, columns=["Configuration",
                                                                     "Total_Energy_per_atom",
                                                                     "OQMD_Total_Energy_per_atom"])

        # Applying all of the above filters with the extra condition that the calculation must have converged
        filtered_values = (relaxation_configuration_filter | standard_configuration_filter) \
                          & energy_threshold_filter & oqmd_material_df["Converged"]

        # Filling our DataFrame at the specified indices
        stable_material_df.loc[(filtered_values, slice(None)), "Configuration"] = \
            oqmd_material_df.loc[filtered_values, "Configuration"].values

        stable_material_df.loc[(filtered_values, slice(None)), "Total_Energy_per_atom"] = \
            per_atom_material_df["Energy_per_atom"]

        stable_material_df.loc[(filtered_values, slice(None)), "OQMD_Total_Energy_per_atom"] = \
            oqmd_material_df.loc[filtered_values, "Total energy [eV/atom]"].values

        # Removing the rows that have uninitialized values because we have filtered out some of them
        stable_material_df.dropna(inplace=True)
        # Appending the individual DataFrames to the list
        frames.append(stable_material_df)

    # The final DataFrame to store all the materials with the same molecule group
    final_df = pd.concat(frames)
    # Storing the DataFrame in a CSV file
    final_df.to_csv(f"./data/post_processed/M{molecule_group}-stable.dat")
