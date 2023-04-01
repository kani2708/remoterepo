import snowflake.connector
import sys
 
#read query from txt file
filename = "C:\\Users\dkanimozhi\Downloads\query.txt"
#filename=sys.argv[1]
#Outputfilepath=sys.argv[2]
Outputfilepath="C:\\Users\dkanimozhi\Downloads\output.txt"
#check=sys.argv[3]
check="1"
file = open(filename, "r")
querytxt = file.read()
file.close()
li=querytxt.split("#")
query=li[0]
medicalgroupquery=li[1]
accountcheckquery=li[2]
print(query)
print(medicalgroupquery)
 
usr = "INTEGRATION_SERVICES_BUILD_DEV" 
pwd = "SystemIntegration0223"
#usr=sys.argv[4]
#pwd=sys.argv[5]
ctx = snowflake.connector.connect(
    user= usr,
    password=pwd,
    #account name is company level
    account='athenahealth',
    #host = account.snowflakecomputing.com
    host='athenahealth.snowflakecomputing.com',
    database='INTEGRATION_SERVICES_DEV',
    schema='ALLCONTEXTS',
    warehouse='INTEGRATION_SERVICES_DEV',
    role='INTEGRATION_SERVICES_DEV_R',
    # use the below property for connecting the snowflake with SSO account
   # authenticator='externalbrowser'
)
cs = ctx.cursor()
if check=='1':
        try:
            cs.execute(accountcheckquery)
            
            rowcount = cs.fetchall()
            rowcount=str(rowcount[0][0])
            f= open(Outputfilepath,"w+")
            f.write(rowcount)
        except IndexError:
                    f= open(Outputfilepath,"w+")
                    f.write("-1")       
        finally:
            cs.close()
            ctx.close()
    
elif check=='2':
        try:
            cs.execute(query)
            
            get = cs.fetchall()
            
           # print(str(get)
            
            
            f= open(Outputfilepath,"w+")
            #f.write(query)
            #f.write(str(get))
            f.write(str(get[0][0]))
            f.write("|")
            f.write(get[0][1])
            #f.write(get[0][2])
            #f.write("|")
            #f.write(str(get[0][3]))
            f.close()
         
        except :
            try:
                cs.execute(medicalgroupquery)
                getname = cs.fetchall()
                if getname=='Null':
                    f= open(Outputfilepath,"w+")
                    f.write("-1")
                else:
                    f= open(Outputfilepath,"w+")
                    f.write(str(getname[0][0]))
                    f.write("|")
                    f.write(getname[0][1])
            except :
                    f= open(Outputfilepath,"w+")
                    f.write("-1")

        finally:
            cs.close()
            ctx.close()