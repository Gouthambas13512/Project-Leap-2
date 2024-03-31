import csv
import pandas as pd
import MasterFunctionfile as mf
import os

#1
#Combine keepa exports and get "need to verify" and Verified

#OUTPUT: Keepa_Export_Combined.csv

input_files = ["KeepaExports\All OOS.csv", "KeepaExports\FBM Only.csv","KeepaExports\FBM.csv"]

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
        
#mf.create_manualcheck_file("North_Face_1_All_PRODUCTS_can_list.csv")   


#3
#Update Master DB with Blacklissted values
#OUtput: No ouput. Just updates
master_db_path = 'Master_DB.csv'
update_csv_path = 'ManualCheck.csv'

#**CODE BELOW---------
#mf.update_BlackList(master_db_path, update_csv_path)
#mf.update_link(master_db_path, update_csv_path)

#4
#Take manual verified and update MasterV and Database
input_file = 'ManualCheck.csv'
master_db_file = 'Master_DB.csv'
master_v_file = 'MasterV.csv'
Black_Listed_Y = 'Black_Listed_Y.csv'  # These are blacklisted Y

#**CODE BELOW---------
#mf.Update_DB_After_ManualV(input_file, master_db_file, master_v_file, Black_Listed_Y)
        
#5
#This takes Keepa_update which is all the verified Listings and updates the masterV with current Buybox:current, current, and store 
#EXPORT this will update MasterV
#**CODE BELOW
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'MasterV.csv')

#6
#Update our selling price (Amazon_List_price)
#EXPORT: Updates MAsterV to itself with the buy box current and current
MasterV = 'MasterV.csv'
master_db = 'Master_DB.csv'

#**CODE BELOW---------
mf.update_pricing_concurrently(MasterV, master_db)



#LIST PRODUCTS ON AMAZON



#7
#Now you want to update pricing of products mid-day with the new items added
#EXPORT this will update MasterV


#**CODE BELOW---------
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'MasterV.csv')


#8
#Update our selling price (Amazon_List_price)
#EXPORT: Updates MAsterV to itself with the buy box current and current
MasterV = 'MasterV.csv'
master_db = 'Master_DB.csv'

#**CODE BELOW---------
#mf.update_pricing_concurrently(MasterV, master_db)

#LIST PRODUCTS ON AMAZON AGAIN
#GO TO SLEEP


#HELPER FUNCTIONS
#Count number of rows
#mf.count_rows("MasterV.csv")

#This is not working
#mf.filter_and_export("MasterV.csv", "New Balance", "New Balance_filtered.csv")










