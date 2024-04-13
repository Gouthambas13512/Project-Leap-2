import csv
import pandas as pd
import MasterFunctionfile as mf
import os

#1
#Combine keepa exports and get "need to verify" and Verified
#OUTPUT: Keepa_Export_Combined.csv
input_files = ["KeepaExports/Newbalance_All.csv"]
# Merge each input file and export only the NEW items not in DB
#OUPUT: Keepa_Combined_Export.csv
"""
for file in input_files:
    if os.path.exists(file):
        mf.merge_csv(file)
    else:
        print(f"File '{file}' not found.")
"""

#1.5
#IF RUNNING THIS, SKIP 2
#Input: Keepa_Combined_Export.csv NEW items which have Black_list and PID column with filled data
#Output: Is the Output_File ready to go directly into MasterDB and MasterV
Prepare_Import_File = "KeepaExports/Keepa_Combined_Export.csv"
Output_File = "KeepaExports/prepared_output.csv"
#mf.prepare_manual_only_import(Prepare_Import_File, Output_File)


#2
#Run Google_Scrapping.py with the output of #1
#It Will the file with name XYZ_Can_LIST.csv -> Send it to MANUAL SCANNER
#mf.create_manualcheck_file("Birkenstock_1_All_PRODUCTS.csv")   


#3
#Update MasterDB link and BlackList from ManualCheck
#Output: No ouput. Just updates MasterDB
#The prepared_output is the file with Black_List and PID column filled out
update_csv_path = "KeepaExports/prepared_output.csv" #'ManualCheck.csv'
master_db_path = 'DataBaseFiles/Master_DB.csv'
#Below should be changed to master_db_path after testing
manual_Update_Ouput = master_db_path
#**CODE BELOW---------
#TRY THIS NEW CODE
#mf.update_master_db_w_manualcheck(master_db_path, update_csv_path, manual_Update_Ouput)
#Below updates masterDB from prepared_output
#mf.update_master_db_without_gogolescrappingpy(master_db_path, update_csv_path, manual_Update_Ouput)


#4
#Take manual verified which is already in DB updates MasterV and Black_Listed_Y file. 
input_file = "KeepaExports/prepared_output.csv"
master_db_file = 'DataBaseFiles/Master_DB.csv'
master_v_file = 'DataBaseFiles/MasterV.csv'
Black_Listed_Y = 'DataBaseFiles/Black_Listed_Y.csv'  # These are blacklisted Y
#**CODE BELOW---------
#mf.Update_DB_After_ManualV(input_file, master_db_file, master_v_file, Black_Listed_Y)

        
#5
#This takes Keepa_update which is all the verified Listings and updates the masterV with current Buybox:current, current, and store 
#Need to pull the updated data from Keepa. In this section you can neglect certain brands
#OUTPUT this will update MasterV
#**CODE BELOW
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'DataBaseFiles\MasterV.csv')
hostname = '54.167.213.74'
#port = 21
username = 'ftpuser'
password = 'Agile@123'
local_file_path = 'DataBaseFiles/MasterV.csv'  # Make sure this is the correct path to your CSV file
remote_dir_path = '/'  # This should be the directory on the server where you want to store your file
remote_file_name = 'MasterV.csv' # The name you want the uploaded file to have
#Updates the CSV path for the UI
#mf.upload_csv_via_ftp(hostname, username, password, local_file_path, remote_dir_path, remote_file_name)


#6
#Tells us if we can list a product. Updates (Amazon_List_price)
#Here we can use filter_and_export to neglect any brand before running
#EXPORT: Updates MasterV to itself
MasterV_Brands_To_Update = 'DataBaseFiles\MasterV.csv'
master_db = 'DataBaseFiles/Master_DB.csv'
Output_File_Price_Update = 'DataBaseFiles\MasterV.csv'
#**CODE BELOW---------
mf.update_pricing_concurrently(MasterV_Brands_To_Update, master_db, MasterV_Brands_To_Update)



#LIST PRODUCTS ON AMAZON

#7
#Now you want to update pricing of products mid-day with the new items added
#EXPORT this will update MasterV


#**CODE BELOW---------
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'DataBaseFiles\MasterV.csv')


#8
#Update our selling price (Amazon_List_price)
#EXPORT: Updates MAsterV to itself with the buy box current and current
MasterV = 'DataBaseFiles\MasterV.csv'
master_db = 'DataBaseFiles\Master_DB.csv'


#**CODE BELOW---------
#mf.update_pricing_concurrently(MasterV, master_db)

#LIST PRODUCTS ON AMAZON AGAIN
#GO TO SLEEP


#HELPER FUNCTIONS
#Count number of rows
#mf.count_rows("DataBaseFiles\MasterV.csv")

#Pulls Certain brands from MasterV file. This Can be used to update pricing without certain brands
UserG = ["Kate Spade", "Kate Spade New York", "New Balance", "Steve Madden", "THE NORTH FACE", "Tory Burch"]
#mf.filter_and_export("DataBaseFiles\MasterV.csv", UserG, "Goutham[DATE].csv")


#HelperFunctions
#mf.update_blacklist_from_master_v("example_asin", "Y")











