"""
Define mappings between dataservice enums and CDS enums
"""
gender_map = {
    "Female": "female",
    "Male": "male",
    "Not Reported": "not reported",
    "Not Available": "unknown",
}
race_map = {
    "White": "white",
    "Black or African American": "black or african american",
    "Native Hawaiian or Other Pacific Islander": "native hawaiian or other pacific islander",  # noqa
    "Asian": "asian",
    "American Indian or Alaska Native": "american indian or alaska native",
    "Reported Unknown": "unknown",
    "Not Available": "unknown",
    "More Than One Race": "other",
    "Other": "other",
    "Not Reported": "not reported",
}
ethnicity_map = {
    "Not Hispanic or Latino": "not hispanic or latino",
    "Hispanic or Latino": "hispanic or latino",
    "Reported Unknown": "unknown",
    "Not Available": "unknown",
    "Not Reported": "not reported",
}
