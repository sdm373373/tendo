# %%
import gspread
import csv

# Define the scope and credentials for accessing the Google Sheet
client = gspread.oauth(
    credentials_filename='credentials.json'
)

# Open the Google Sheet
sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1YTF2uF-XpXdwK4GG6nlpopAYg5wzJhTACjh0Ib_A2BQ/edit#gid=416190132')

# Iterate over each worksheet in the Google Sheet
for worksheet in sheet.worksheets():
    # Get the name of the worksheet
    worksheet_name = worksheet.title
    
    # Get the data from the worksheet
    data = worksheet.get_all_values()
    
    # Create a CSV file with the same name as the worksheet
    with open(f'{worksheet_name}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write each row of data to the CSV file
        for row in data:
            writer.writerow(row)


# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tendo_1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

dfEncounters = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("encounter_e1.csv")
dfLabs = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("lab_e1.csv")
dfMedications = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("medications_e1.csv")
dfPatients = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("patient_e1.csv")

dfEncounters.createOrReplaceTempView("encounters")
dfLabs.createOrReplaceTempView("labs")
dfMedications.createOrReplaceTempView("medications")
dfPatients.createOrReplaceTempView("patients")

# %%
import datetime

dfResult1 = spark.sql("""
    SELECT 
        p.patientid as patient_id,
        p.Sex,
        p.Age,
        p.primary_care_provider,
        coalesce(m.medication_simple_generic_name, '') as medication_simple_generic_name,
        AVG(coalesce(m.minimum_dose, 0.0)) as avg_minimum_dose,
        coalesce(m.dose_unit, '') as dose_unit,
        MAX(e.admit_diagnosis) as admit_diagnosis
    FROM patients p
    INNER JOIN medications m on p.patientid = m.patientid
    INNER JOIN encounters e on p.patientid = e.patientid
    GROUP BY p.patientid, p.Sex, p.Age, p.primary_care_provider, m.medication_simple_generic_name, m.dose_unit
    order by p.patientid, p.Sex, p.Age, p.primary_care_provider, m.medication_simple_generic_name, m.dose_unit
""")
                      
# Get current date in "YYYYMMDD" format
current_date = datetime.datetime.now().strftime("%Y%m%d")

# Step 3: Create the filename
filename = f"target_1_{current_date}.txt"
                      
# dfResult1.write.format("csv").option("header", "true").option("delimiter", "|").option("encoding", "UTF-8").mode("overwrite").save(filename)
dfResult1.toPandas().to_csv(filename, sep='|', encoding='utf-8', index=False, quoting=csv.QUOTE_ALL, lineterminator='\n')

dfResult1.show()


