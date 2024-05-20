import csv
import pandas as pd
import MasterFunctionfile as mf
import os

#1
#Combine keepa exports and get "need to verify" and Verified
#OUTPUT: Keepa_Export_Combined.csv
input_files = [r"KeepaExports\file1.csv",r"KeepaExports\file2.csv"]
# Merge each input file and export only the NEW items not in DB
#OUPUT: Keepa_Combined_Export.csv

#mf.combine_and_filter_exports(input_files=input_files, master_file_path="DataBaseFiles/Master_DB.csv",blacklist_file_path="DataBaseFiles/Black_Listed_Y.csv",output_file="Keepa_Combined_Export.csv")


#1.5
#IF RUNNING THIS, SKIP 2
#Input: Keepa_Combined_Export.csv NEW items which have Black_list and PID column with filled data
#Output: Is the Output_File ready to go directly into MasterDB and MasterV
Prepare_Import_File = "KeepaExports\Keepa_Combined_Export.csv"
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
#mf.update_master_db_w_manualcheck(master_db_path, update_csv_path)

#Below updates masterDB from prepared_output
#mf.update_master_db_without_gogolescrappingpy(master_db_path, update_csv_path,manual_Update_Ouput)


#4
#Take manual verified which is already in DB updates MasterV and Black_Listed_Y file. 
input_file = "KeepaExports/prepared_output.csv"
master_db_file = 'DataBaseFiles/Master_DB.csv'
master_v_file = 'DataBaseFiles/MasterV.csv'
Black_Listed_Y = 'DataBaseFiles/Black_Listed_Y.csv'  # These are blacklisted Y
#**CODE BELOW---------
#mf.Update_DB_After_ManualV(input_file, master_db_file, master_v_file, Black_Listed_Y)


#6
#Tells us if we can list a product. Updates (Amazon_List_price)
#Here we can use filter_and_export to neglect any brand before running
#EXPORT: Updates MasterV to itself
MasterV_Brands_To_Update = 'DataBaseFiles\MasterV.csv'
master_db = 'DataBaseFiles/Master_DB.csv'
Output_File_Price_Update = 'DataBaseFiles\MasterV.csv'
Brands1 = ['New Balance',' Tory Burch', 'LifeStride',' Nike', 'Champion','TYR']
#Brands2 = ['Birkenstock', 'Steve Madden', 'Free People', 'Columbia', 'Polo Ralph Lauren','POLO RALPH LAUREN' 'Skechers', 'Cole Haan']
#Brands3 =['Lacoste', 'Laura Mercier', 'Kate Spade New York', 'Cole Haan', 'Buffalo David Bitton', 'Kate Spade','TOMMY HILFIGER','Tommy Hilfiger']
'''
df = pd.read_csv(MasterV_Brands_To_Update)
unique_brands = df['Brand'].unique()
print(unique_brands)
'''
filename = 'DataBaseFiles\MasterV.csv'
columns_to_clean = ['Price', 'Min Price', 'Max Price', 'Amazon_List_price']

#**CODE BELOW---------

# This filters out specific brands so you run keepa with just those
#mf.keepa_asin_import(Brands1)

# Combine the CSV outputs from keepa into one file
#mf.combine_csv_files(r"KeepaExports\Scan1.csv", r"KeepaExports\Scan2.csv")

#This takes Keepa_update and updates the masterV with current Buybox:current, current, and store 
#mf.update_master_v('KeepaExports\Keepa_Update.csv', 'DataBaseFiles\MasterV.csv')

#This will remove Pricing of the brands listed last time
#mf.remove_numeric_values(filename, columns_to_clean)

#mf.update_pricing_concurrently(MasterV_Brands_To_Update, master_db, Output_File_Price_Update,Brands1)

#mf.update_amazon_listing_price('DataBaseFiles/MasterV.csv', 'DataBaseFiles/MasterV.csv')

#mf.upload_file(r"C:\Users\Administrator\Documents\RA\DataBaseFiles\MasterV.csv")

#It corrects UPC problem
#mf.update_master_v_UPC(r'DataBaseFiles\MasterV.csv', r"KeepaExports\Update_UPC_1.csv", r'DataBaseFiles\MasterV.csv')

#use below to create excel sheet
#mf.update_excel_from_csv('DataBaseFiles/MasterV.csv', 'Amazon_Upload_xlsx\Ever_Upload.xlsx')




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



