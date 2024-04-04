import csv
import pandas as pd
import MasterFunctionfile as mf
import os

#1
#Combine keepa exports and get "need to verify" and Verified

#OUTPUT: Keepa_Export_Combined.csv

input_files = ["KeepaExports\All IS.csv", "KeepaExports\All OOS.csv","KeepaExports\FBA and AMZ Only.csv", "KeepaExports\FBM Only.csv"]

# Merge each input file
#OUPUT: Keepa_Combined_Export.csv
'''
for file in input_files:
    if os.path.exists(file):
        mf.merge_csv(file)
    else:
        print(f"File '{file}' not found.")
'''

#2
#Run Google_Scrapping.py with the output of #1
#It Will the file with name XYZ_Can_LIST.csv -> Send it to MANUAL SCANNER
#mf.create_manualcheck_file("Birkenstock_1_All_PRODUCTS.csv")   


#3
#Update MasterDB link and BlackList from ManualCheck
#Output: No ouput. Just updates MasterDB
master_db_path = 'DataBaseFiles\Master_DB.csv'
update_csv_path = 'ManualCheck_birkenstock.csv'
#**CODE BELOW---------
#TRY THIS NEW CODE
#mf.update_master_db_w_manualcheck(master_db_path, update_csv_path)
#OLD CODE
#mf.update_BlackList(master_db_path, update_csv_path)
#mf.update_link(master_db_path, update_csv_path)

#4
#Take manual verified and update MasterV and Database
input_file = 'ManualCheck_birkenstock.csv'
master_db_file = 'DataBaseFiles\Master_DB.csv'
master_v_file = 'DataBaseFiles\MasterV.csv'
Black_Listed_Y = 'DataBaseFiles\Black_Listed_Y.csv'  # These are blacklisted Y
#**CODE BELOW---------
#mf.Update_DB_After_ManualV(input_file, master_db_file, master_v_file, Black_Listed_Y)
        
#5
#This takes Keepa_update which is all the verified Listings and updates the masterV with current Buybox:current, current, and store 
#Need to pull the updated data from Keepa. In this section you can neglect certain brands
#OUTPUT this will update MasterV
#**CODE BELOW
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'DataBaseFiles\MasterV.csv')

#6
#Tells us if we can list a product. Updates (Amazon_List_price)
#Here we can use filter_and_export to neglect any brand before running
#EXPORT: Updates MasterV to itself
MasterV_Brands_To_Update = 'DataBaseFiles\MasterV.csv'
master_db = 'DataBaseFiles\Master_DB.csv'
#**CODE BELOW---------
mf.update_pricing_concurrently(MasterV_Brands_To_Update, master_db)

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


#mf.update_extraction_links('ManualCheck_birkenstock.csv')







