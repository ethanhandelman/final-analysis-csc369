**Data Parsing/Quality Issues**
- The data was only recorded from Feb 2022 to Jun 2023, but I am not too worried about this as there is still plenty
of it and I intend this to be more of a proof of concept for identifying bot campaigns
- Some of the earlier csv files did not have all 29 fields, so I cut down the columns inputted into the DuckDB so that
they were all compatible, which I was going to do anyways as only 10 columns were relevant
- Some of the user account created dates were impossible (such as 1970) which I attribute to people's client device
time being off when signing up, but I don't believe this will affect the data much as I wrote a script to determine
that just 67 users were "created" before 2005 representing just 0.000107% of users within the dataset