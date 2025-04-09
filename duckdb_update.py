import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
from alpha_vantage.timeseries import TimeSeries

# Connect to duckdb db
con = duckdb.connect(database='./etf.duckdb', read_only=False)

# Connect to Alpha Vantage
ts = TimeSeries(key='35RWNNYNVVW23JS1', output_format='pandas')

# Load ET Data
ET, meta_data = ts.get_daily(symbol='ET', outputsize='full')
# 🔹 Rename columns for clarity
ET.columns = ["Open", "High", "Low", "Close", "Volume"]

# 🔹 Convert index to datetime format
ET.index = pd.to_datetime(ET.index)

# 🔹 Convert index (date) into a regular column
ET.reset_index(inplace=True)

# Calculate 100-day moving average for Close price
# ET["100d_MA"] = ET["Close"].rolling(window=100).mean()

# 🔹 Rename the date column
ET.rename(columns={"date": "Date"}, inplace=True)
ET['Date'].max()

con.register('temp_df', ET)  # Register the DataFrame as a virtual table
con.execute("DROP TABLE IF EXISTS ET")
con.execute("CREATE TABLE ET AS SELECT * FROM temp_df")  # Create the table
con.unregister('temp_df') #unregister the temp table.

# Load BND Data

BND, meta_data = ts.get_daily(symbol='BND', outputsize='full')
# 🔹 Rename columns for clarity
BND.columns = ["Open", "High", "Low", "Close", "Volume"]

# 🔹 Convert index to datetime format
BND.index = pd.to_datetime(BND.index)

# 🔹 Convert index (date) into a regular column
BND.reset_index(inplace=True)

# Calculate 100-day moving average for Close price
# BND["100d_MA"] = BND["Close"].rolling(window=100).mean()

# 🔹 Rename the date column
BND.rename(columns={"date": "Date"}, inplace=True)

con.register('temp_df', BND)  # Register the DataFrame as a virtual table
con.execute("DROP TABLE IF EXISTS BND")
con.execute("CREATE TABLE BND AS SELECT * FROM temp_df")  # Create the table
con.unregister('temp_df') #unregister the temp table.

# Pull VTI data

VTI, meta_data = ts.get_daily(symbol='VTI', outputsize='full')
# 🔹 Rename columns for clarity
VTI.columns = ["Open", "High", "Low", "Close", "Volume"]

# 🔹 Convert index to datetime format
VTI.index = pd.to_datetime(VTI.index)

# 🔹 Convert index (date) into a regular column
VTI.reset_index(inplace=True)

# Calculate 100-day moving average for Close price
# VTI["100d_MA"] = VTI["Close"].rolling(window=100).mean()

# 🔹 Rename the date column
VTI.rename(columns={"date": "Date"}, inplace=True)

con.register('temp_df', VTI)  # Register the DataFrame as a virtual table
con.execute("DROP TABLE IF EXISTS VTI")
con.execute("CREATE TABLE VTI AS SELECT * FROM temp_df")  # Create the table
con.unregister('temp_df') #unregister the temp table.

# Load BNDX data

BNDX, meta_data = ts.get_daily(symbol='BNDX', outputsize='full')
# 🔹 Rename columns for clarity
BNDX.columns = ["Open", "High", "Low", "Close", "Volume"]

# 🔹 Convert index to datetime format
BNDX.index = pd.to_datetime(BNDX.index)

# 🔹 Convert index (date) into a regular column
BNDX.reset_index(inplace=True)

# Calculate 100-day moving average for Close price
# BNDX["100d_MA"] = BNDX["Close"].rolling(window=100).mean()

# 🔹 Rename the date column
BNDX.rename(columns={"date": "Date"}, inplace=True)

con.register('temp_df', BNDX)  # Register the DataFrame as a virtual table
con.execute("DROP TABLE IF EXISTS BNDX")
con.execute("CREATE TABLE BNDX AS SELECT * FROM temp_df")  # Create the table
con.unregister('temp_df') #unregister the temp table.

# Load VXUS Data

VXUS, meta_data = ts.get_daily(symbol='VXUS', outputsize='full')
# 🔹 Rename columns for clarity
VXUS.columns = ["Open", "High", "Low", "Close", "Volume"]

# 🔹 Convert index to datetime format
VXUS.index = pd.to_datetime(VXUS.index)

# 🔹 Convert index (date) into a regular column
VXUS.reset_index(inplace=True)

# Calculate 100-day moving average for Close price
# VXUS["100d_MA"] = VXUS["Close"].rolling(window=100).mean()

# 🔹 Rename the date column
VXUS.rename(columns={"date": "Date"}, inplace=True)

con.register('temp_df', VXUS)  # Register the DataFrame as a virtual table
con.execute("DROP TABLE IF EXISTS VXUS")
con.execute("CREATE TABLE VXUS AS SELECT * FROM temp_df")  # Create the table
con.unregister('temp_df') #unregister the temp table.




