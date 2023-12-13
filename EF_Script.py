from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep, time
import re
import os,sys
import multiprocessing
from site_scraping import Site_Scrapa
import mysql.connector
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def Update_Token(mydb,total_token,output_file):
    output_file = open(output_file,"r")
    ouput_file_len = len(output_file.readlines())
    let_token = total_token-ouput_file_len
    all_argv = sys.argv
    mycursor = mydb.cursor()
    ssl = "UPDATE Users_Token set Total_Token ='{}' where User_Email = '{}'".format(let_token,all_argv[2])
    mycursor.execute(ssl)
    mydb.commit()
    mycursor.close()

def check_Tokens(mydb):
    all_argv = sys.argv
    mycursor = mydb.cursor()
    mycursor.execute("SELECT Total_Token FROM Users_Token where User_Email = '{}'".format(all_argv[2]))
    Token = mycursor.fetchall()
    if len(Token) > 0:
        mycursor.close()
        tokens = Token[0][0]
        print(f"You Have Total {tokens} Tokens")
        return tokens
    else:
        print("Please Verity Your Account To Admin")
        sys.exit()

def gsprd_data(credential_file_patha):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file_patha,scope)
    client = gspread.authorize(credentials)
    sheet_title = sys.argv[1]
    sheet = client.open_by_url(sheet_title)

    # Select the second sheet (worksheet) by index (index starts from 0)
    worksheet_index = 0
    worksheet = sheet.get_worksheet(worksheet_index)
    rows = worksheet.get_all_values()
    column_data = [row[0] for row in rows]
    return column_data

def apply_thread(U_lista):
    out_f =  open(Output_file,"r")
    cout_out = len(out_f.readlines())
    futures = []
    with ThreadPoolExecutor() as executor:
            for Url_i in U_lista:
                if cout_out < Totol_Tokens:
                    Url_i =  Url_i.replace("\n",'')
                    futures.append(
                        executor.submit(Site_Scrapa,Url_i)
                    )
                else:
                    return    
    wait(futures)

if "__main__"== __name__:
    start_time = time()
    #################### Script-path ##################

    Script_path = os.path.dirname(os.path.abspath(__file__))
    Input_folder = os.path.join(Script_path,"Input")
    Domain_file = os.path.join(Input_folder,"Domain_file.csv")
    Url_file = os.path.join(Input_folder,"Url_file.csv")
    Filter_file = os.path.join(Input_folder,"Filter_.csv")
    Output_file =  os.path.join(Script_path,"Output_.csv")
    credential_file_path = os.path.join(Input_folder,"Credential_File.json")

    ################## Data Base ########################

    mydb = mysql.connector.connect(
        host = "3.140.99.156",
        username = "wp_raj1",
        password = "rajPassword95$",
        database = "Find_Email_Script"
    )

    ################### Processing #######################

    Totol_Tokens = check_Tokens(mydb)

    if Totol_Tokens == 0:
        sys.exit()

    ######################################################

    O_file = open(Output_file,"w")
    O_file.close()
    
    U_file = open(Url_file,'w')
    D_list = gsprd_data(credential_file_path)

    for domain_i in D_list:
        domain_i = domain_i.replace("\n",'')
        s_url = "https://{}/".format(domain_i)
        U_file.write(s_url+"\n")
    U_file.close()    

    U_file = open(Url_file,"r")
    U_l = U_file.readlines()
    U_list = []

    for U_i in U_l:
        U_i = U_i.replace("\n",'')
        U_list.append(U_i)

    pair_list = []
    cpu_count = 4

    for i in range(1,cpu_count):
        start_i = int(len(U_list) * (i-1)/cpu_count)
        end_i = int(len(U_list) * (i)/cpu_count)
        aa = U_list[start_i:end_i]
        pair_list.append(aa)
    
    pool = multiprocessing.Pool()

    # Apply the square function to each number in parallel
    results = pool.map(apply_thread,pair_list,)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()  
    ######################## Add Filter #######################

    Filtered_Data = []
    
    F_file = open(Filter_file,"r")
    filter_f = F_file.readlines()

    O_file = open(Output_file,'r')
    Output_file1=  O_file.readlines()
    for O_i in Output_file1:
        for f_i in filter_f:
            if f_i not in O_i:
                Filtered_Data.append(O_i)
    O_file.close()

    Ouput_file_f = open(Output_file,'w')
    for oo_i in list(set(Filtered_Data)):
        Ouput_file_f.write(oo_i)
    Ouput_file_f.close()
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time: {elapsed_time} seconds")
    Update_Token(mydb,Totol_Tokens,Output_file)