# BHRC
This repository contains the latest stage of my Bachelor Honours Research Project.

BHRC_dataManagement and BHRC_dataPuller are the main bulk of the code and the data collection part of the project. the dataPuller file requests data from the Morningstar API for a given quarter. It then recursively expands any funds contained in each parent fund to its weight adjusted corresponding holdings (limited to funds that have available data on Morningstar). This then gets handled in a merging and cleaning pipeline using duckDB, as the API only handles 900 funds at a time and occasionally has duplicated due to non-fixed reporting dates. I used Copilot to document the final code and make slight efficiency improvements.

Attached under release v1.0 with name "Upload" is the parquet output for 2025-01-30 as a sample output.

CSV files contain a list of SECIDs for the funds being analyzed. I started with funds of average net assets over lifespan greater 10 billion USD (csv not included as it contains data from Morningstar) and then increased my search to funds with average net assets greater than 1 billion. These are all US based asset allocation funds.

datapoints is the script I used to choose which specific data points I wanted to pull from the API.

RNDdensityTest was just a quick curiosity into risk neutral densities that I investigated for a day or two but did not continue. It gave me insights for future research.
